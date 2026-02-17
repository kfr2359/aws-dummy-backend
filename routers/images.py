import io
import mimetypes
import random
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from db import Image, get_db
import s3_client


router = APIRouter()


class ImageMetadata(BaseModel):
    last_updated_at: datetime
    name: str
    size_bytes: int
    extension: str


class DeleteResult(BaseModel):
    name: str
    deleted: bool


def _guess_content_type(name: str, extension: str) -> str:
    content_type, _ = mimetypes.guess_type(f"{name}.{extension}")
    return content_type or "application/octet-stream"


def _get_by_name(db: Session, name: str) -> Image:
    image = db.execute(select(Image).where(Image.name == name)).scalar_one_or_none()
    if not image:
        raise HTTPException(status_code=404, detail="Image not found")
    return image


@router.get("/random/metadata", response_model=ImageMetadata)
def get_random_metadata(db: Session = Depends(get_db)):
    total = db.execute(select(func.count(Image.id))).scalar_one()
    if total == 0:
        raise HTTPException(status_code=404, detail="No images available")

    offset = random.randrange(total)
    image = (
        db.execute(select(Image).order_by(Image.id).offset(offset).limit(1))
        .scalar_one()
    )

    return ImageMetadata(
        last_updated_at=image.last_updated_at,
        name=image.name,
        size_bytes=image.size_bytes,
        extension=image.extension,
    )


@router.get("/{name}/metadata", response_model=ImageMetadata)
def get_metadata(name: str, db: Session = Depends(get_db)):
    image = _get_by_name(db, name)
    return ImageMetadata(
        last_updated_at=image.last_updated_at,
        name=image.name,
        size_bytes=image.size_bytes,
        extension=image.extension,
    )


@router.get("/{name}")
def download_by_name(name: str, db: Session = Depends(get_db)):
    image = _get_by_name(db, name)

    try:
        obj = s3_client.download_image(key=image.s3_key)
    except Exception as e:  # boto3 raises various exception types
        raise HTTPException(status_code=502, detail="Failed to download from S3") from e

    body = obj["Body"]
    content_type = obj.get("ContentType") or _guess_content_type(image.name, image.extension)

    def iter_chunks():
        while True:
            chunk = body.read(1024 * 1024)
            if not chunk:
                break
            yield chunk

    headers = {
        "Content-Disposition": f'attachment; filename="{image.name}.{image.extension}"'
    }
    return StreamingResponse(iter_chunks(), media_type=content_type, headers=headers)


@router.post("", response_model=ImageMetadata)
def upload(
    db: Session = Depends(get_db),
    file: UploadFile = File(...),
    name: Optional[str] = Form(default=None),
):
    filename = file.filename or ""
    path = Path(filename)

    try:
        extension = s3_client.normalize_extension(path.suffix or "")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    final_name = (name or path.stem or "").strip()
    if not final_name:
        raise HTTPException(status_code=400, detail="Image name is required")

    try:
        key = s3_client.generate_s3_key(final_name, extension)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    try:
        data = file.file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail="Failed to read upload") from e

    size_bytes = len(data)
    if size_bytes == 0:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    content_type = file.content_type or _guess_content_type(final_name, extension)

    try:
        s3_client.upload_image(fileobj=io.BytesIO(data), key=key, content_type=content_type)
    except Exception as e:
        raise HTTPException(status_code=502, detail="Failed to upload to S3") from e

    # Upsert-like behavior: overwrite existing record if present.
    existing = db.execute(select(Image).where(Image.name == final_name)).scalar_one_or_none()
    if existing:
        existing.size_bytes = size_bytes
        existing.extension = extension
        existing.s3_key = key
        db.add(existing)
        db.commit()
        db.refresh(existing)
        image = existing
    else:
        image = Image(
            name=final_name,
            size_bytes=size_bytes,
            extension=extension,
            s3_key=key,
        )
        db.add(image)
        try:
            db.commit()
        except IntegrityError as e:
            db.rollback()
            raise HTTPException(status_code=409, detail="Image name already exists") from e
        db.refresh(image)

    return ImageMetadata(
        last_updated_at=image.last_updated_at,
        name=image.name,
        size_bytes=image.size_bytes,
        extension=image.extension,
    )


@router.delete("/{name}", response_model=DeleteResult)
def delete_by_name(name: str, db: Session = Depends(get_db)):
    image = _get_by_name(db, name)

    try:
        s3_client.delete_image(key=image.s3_key)
    except Exception as e:
        raise HTTPException(status_code=502, detail="Failed to delete from S3") from e

    db.delete(image)
    db.commit()
    return DeleteResult(name=name, deleted=True)

