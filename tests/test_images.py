import io
import os
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

import db as db_module
import s3_client as s3_module
from main import app


class _FakeS3Body:
    def __init__(self, data: bytes):
        self._buf = io.BytesIO(data)

    def read(self, n: int):
        return self._buf.read(n)


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("AWS_S3_BUCKET", "test-bucket")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("DB_URL", "sqlite+pysqlite:///:memory:")


@pytest.fixture()
def client(monkeypatch):
    # Ensure db module uses an in-memory engine for this test run.
    db_module._ENGINE = None
    db_module._SESSION_FACTORY = None

    # Patch S3 operations.
    store = {}

    def fake_upload_image(*, fileobj, key, content_type):
        store[key] = fileobj.read()

    def fake_download_image(*, key):
        if key not in store:
            raise KeyError("missing")
        return {"Body": _FakeS3Body(store[key]), "ContentType": "image/png"}

    def fake_delete_image(*, key):
        store.pop(key, None)

    monkeypatch.setattr(s3_module, "upload_image", fake_upload_image)
    monkeypatch.setattr(s3_module, "download_image", fake_download_image)
    monkeypatch.setattr(s3_module, "delete_image", fake_delete_image)

    # Initialize DB tables (startup event might not run in tests)
    db_module.init_db(db_module.get_engine())

    return TestClient(app)


def test_upload_and_metadata_and_download_and_delete(client):
    # Upload
    resp = client.post(
        "/images",
        files={"file": ("hello.png", b"PNGDATA", "image/png")},
        data={"name": "hello"},
    )
    assert resp.status_code == 200, resp.text
    meta = resp.json()
    assert meta["name"] == "hello"
    assert meta["extension"] == "png"
    assert meta["size_bytes"] == 7
    assert "last_updated_at" in meta

    # Metadata
    resp = client.get("/images/hello/metadata")
    assert resp.status_code == 200, resp.text
    meta2 = resp.json()
    assert meta2["name"] == "hello"
    assert meta2["extension"] == "png"

    # Random metadata
    resp = client.get("/images/random/metadata")
    assert resp.status_code == 200, resp.text
    rand_meta = resp.json()
    assert rand_meta["name"] == "hello"

    # Download
    resp = client.get("/images/hello")
    assert resp.status_code == 200, resp.text
    assert resp.content == b"PNGDATA"
    assert resp.headers["content-type"].startswith("image/")

    # Delete
    resp = client.delete("/images/hello")
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"name": "hello", "deleted": True}

    # Gone
    resp = client.get("/images/hello/metadata")
    assert resp.status_code == 404

