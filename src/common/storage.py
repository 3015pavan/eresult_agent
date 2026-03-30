"""
Object storage abstraction for AcadExtract.

Supports:
  - MinIO / S3-compatible storage
  - Supabase Storage (REST API)
"""

from __future__ import annotations

import io
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Protocol

import httpx

logger = logging.getLogger(__name__)

try:
    from minio import Minio
    from minio.error import S3Error
    _HAS_MINIO = True
except ImportError:
    _HAS_MINIO = False
    Minio = None
    S3Error = Exception


class StorageBackend(Protocol):
    def ensure_buckets(self) -> None: ...
    def store_email(self, message_id: str, email_dict: dict) -> str: ...
    def get_email(self, path: str) -> dict: ...
    def store_attachment(self, email_id: str, filename: str, data: bytes) -> str: ...


def _safe_object_name(name: str) -> str:
    return (
        name.replace("\\", "_")
        .replace("/", "_")
        .replace("<", "")
        .replace(">", "")
        .replace(" ", "_")
    )


class MinIOStorage:
    """Wrapper around MinIO client with bucket management."""

    def __init__(self):
        from src.common.config import get_settings

        cfg = get_settings().storage
        self.bucket_emails = cfg.minio_bucket_emails
        self.bucket_attachments = cfg.minio_bucket_attachments
        self._client: Optional[Minio] = None
        self._endpoint = cfg.minio_endpoint
        self._access_key = cfg.minio_access_key
        self._secret_key = cfg.minio_secret_key
        self._secure = cfg.minio_secure

    @property
    def client(self) -> Minio:
        if not _HAS_MINIO:
            raise RuntimeError("minio package not installed")
        if self._client is None:
            self._client = Minio(
                self._endpoint,
                access_key=self._access_key,
                secret_key=self._secret_key,
                secure=self._secure,
            )
        return self._client

    def ensure_buckets(self) -> None:
        for bucket in (self.bucket_emails, self.bucket_attachments):
            if not self.client.bucket_exists(bucket):
                self.client.make_bucket(bucket)
                logger.info("Created MinIO bucket: %s", bucket)

    def store_email(self, message_id: str, email_dict: dict) -> str:
        now = datetime.now(timezone.utc)
        safe_id = _safe_object_name(message_id)
        object_path = f"{now.year}/{now.month:02d}/{safe_id}.json"
        payload = json.dumps(email_dict, ensure_ascii=False, default=str).encode("utf-8")
        data = io.BytesIO(payload)
        self.client.put_object(
            self.bucket_emails,
            object_path,
            data,
            length=len(payload),
            content_type="application/json",
        )
        return f"s3://{self.bucket_emails}/{object_path}"

    def get_email(self, path: str) -> dict:
        object_path = path.split("/", 3)[-1] if path.startswith("s3://") else path
        response = self.client.get_object(self.bucket_emails, object_path)
        data = response.read()
        return json.loads(data.decode("utf-8"))

    def store_attachment(self, email_id: str, filename: str, data: bytes) -> str:
        object_path = f"{_safe_object_name(email_id)}/{_safe_object_name(filename)}"
        self.client.put_object(
            self.bucket_attachments,
            object_path,
            io.BytesIO(data),
            length=len(data),
        )
        return f"s3://{self.bucket_attachments}/{object_path}"

    def presigned_url(self, bucket: str, object_path: str, expires_hours: int = 1) -> str:
        return self.client.presigned_get_object(
            bucket,
            object_path,
            expires=timedelta(hours=expires_hours),
        )


class SupabaseStorage:
    """Minimal Supabase Storage client using REST endpoints."""

    def __init__(self):
        from src.common.config import get_settings

        cfg = get_settings().storage
        if not cfg.supabase_configured:
            raise RuntimeError("Supabase storage not configured")
        self.base_url = cfg.supabase_url.rstrip("/")
        self.service_key = cfg.supabase_service_key
        self.bucket_emails = cfg.supabase_bucket_emails
        self.bucket_attachments = cfg.supabase_bucket_attachments
        self._client = httpx.Client(
            timeout=30.0,
            headers={
                "Authorization": f"Bearer {self.service_key}",
                "apikey": self.service_key,
            },
        )

    def _object_url(self, bucket: str, object_path: str) -> str:
        return f"{self.base_url}/storage/v1/object/{bucket}/{object_path}"

    def _bucket_url(self) -> str:
        return f"{self.base_url}/storage/v1/bucket"

    def ensure_buckets(self) -> None:
        for bucket in (self.bucket_emails, self.bucket_attachments):
            try:
                resp = self._client.post(
                    self._bucket_url(),
                    json={"id": bucket, "name": bucket, "public": False},
                )
                if resp.status_code not in (200, 201, 409):
                    resp.raise_for_status()
            except Exception as exc:
                logger.warning("supabase_bucket_ensure_failed %s: %s", bucket, exc)
                raise

    def store_email(self, message_id: str, email_dict: dict) -> str:
        now = datetime.now(timezone.utc)
        object_path = f"{now.year}/{now.month:02d}/{_safe_object_name(message_id)}.json"
        payload = json.dumps(email_dict, ensure_ascii=False, default=str).encode("utf-8")
        resp = self._client.post(
            self._object_url(self.bucket_emails, object_path),
            headers={"x-upsert": "true", "content-type": "application/json"},
            content=payload,
        )
        resp.raise_for_status()
        return f"supabase://{self.bucket_emails}/{object_path}"

    def get_email(self, path: str) -> dict:
        object_path = path.split("/", 3)[-1] if path.startswith("supabase://") else path
        resp = self._client.get(self._object_url(self.bucket_emails, object_path))
        resp.raise_for_status()
        return resp.json() if isinstance(resp.json(), dict) else json.loads(resp.text)

    def store_attachment(self, email_id: str, filename: str, data: bytes) -> str:
        object_path = f"{_safe_object_name(email_id)}/{_safe_object_name(filename)}"
        resp = self._client.post(
            self._object_url(self.bucket_attachments, object_path),
            headers={"x-upsert": "true", "content-type": "application/octet-stream"},
            content=data,
        )
        resp.raise_for_status()
        return f"supabase://{self.bucket_attachments}/{object_path}"


class UnifiedStorage:
    """Facade selecting MinIO or Supabase based on configuration."""

    def __init__(self):
        from src.common.config import get_settings

        cfg = get_settings().storage
        if cfg.backend.lower() == "supabase" and cfg.supabase_configured:
            self.backend: StorageBackend = SupabaseStorage()
        else:
            self.backend = MinIOStorage()

    def ensure_buckets(self) -> None:
        self.backend.ensure_buckets()

    def store_email(self, message_id: str, email_dict: dict) -> str:
        return self.backend.store_email(message_id, email_dict)

    def get_email(self, path: str) -> dict:
        return self.backend.get_email(path)

    def store_attachment(self, email_id: str, filename: str, data: bytes) -> str:
        return self.backend.store_attachment(email_id, filename, data)


_storage: Optional[UnifiedStorage] = None


def get_storage() -> UnifiedStorage:
    global _storage
    if _storage is None:
        _storage = UnifiedStorage()
    return _storage
