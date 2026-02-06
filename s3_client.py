import os
from dataclasses import dataclass
from typing import IO, Optional

import boto3


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def sanitize_image_name(name: str) -> str:
    """
    Keep it S3-key-safe and predictable.
    """
    name = name.strip()
    if not name:
        raise ValueError("Image name must not be empty")

    # Allow only a conservative set of characters
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
    cleaned = "".join(ch for ch in name if ch in allowed)
    if not cleaned:
        raise ValueError("Image name contains no allowed characters")
    return cleaned


def normalize_extension(ext: str) -> str:
    ext = ext.strip().lower().lstrip(".")
    if not ext:
        raise ValueError("File extension is required")
    return ext


def generate_s3_key(name: str, extension: str) -> str:
    name = sanitize_image_name(name)
    extension = normalize_extension(extension)
    return f"images/{name}.{extension}"


@dataclass(frozen=True)
class S3Config:
    bucket: str
    region: Optional[str]


def get_s3_config() -> S3Config:
    return S3Config(
        bucket=_require_env("AWS_S3_BUCKET"),
        region=os.getenv("AWS_REGION"),
    )


def get_s3_client():
    cfg = get_s3_config()
    if cfg.region:
        return boto3.client("s3", region_name=cfg.region)
    return boto3.client("s3")


def upload_image(*, fileobj: IO[bytes], key: str, content_type: Optional[str]) -> None:
    cfg = get_s3_config()
    client = get_s3_client()
    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type
    if extra_args:
        client.upload_fileobj(fileobj, cfg.bucket, key, ExtraArgs=extra_args)
    else:
        client.upload_fileobj(fileobj, cfg.bucket, key)


def download_image(*, key: str):
    """
    Returns the raw S3 get_object response (includes streaming Body).
    """
    cfg = get_s3_config()
    client = get_s3_client()
    return client.get_object(Bucket=cfg.bucket, Key=key)


def delete_image(*, key: str) -> None:
    cfg = get_s3_config()
    client = get_s3_client()
    client.delete_object(Bucket=cfg.bucket, Key=key)

