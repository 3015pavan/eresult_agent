"""
MinIO object storage layer.

Stores raw email JSON and attachment bytes in S3-compatible object storage.
Buckets:
  emails-raw    — {year}/{month}/{message_id}.json
  attachments   — {email_id}/{filename}
"""

from __future__ import annotations

import io
import json
import logging
import os
from datetime import datetime
from typing import Optional

from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)

BUCKET_EMAILS = os.getenv("MINIO_BUCKET_EMAILS", "emails-raw")
BUCKET_ATTACHMENTS = os.getenv("MINIO_BUCKET_ATTACHMENTS", "attachments")


def _get_client() -> Minio:
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


class MinIOStorage:
    """Wrapper around Minio client with bucket management and retry logic."""

    def __init__(self):
        self._client: Optional[Minio] = None

    @property
    def client(self) -> Minio:
        if self._client is None:
            self._client = _get_client()
        return self._client

    # ── Bucket management ─────────────────────────────────────────────────────

    def ensure_buckets(self) -> None:
        """Create required buckets if they don't exist yet."""
        for bucket in (BUCKET_EMAILS, BUCKET_ATTACHMENTS):
            try:
                if not self.client.bucket_exists(bucket):
                    self.client.make_bucket(bucket)
                    logger.info("Created MinIO bucket: %s", bucket)
                else:
                    logger.debug("MinIO bucket already exists: %s", bucket)
            except S3Error as exc:
                logger.error("Failed to create/check bucket %s: %s", bucket, exc)
                raise

    # ── Email storage ─────────────────────────────────────────────────────────

    def store_email(self, message_id: str, email_dict: dict) -> str:
        """
        Persist the raw email dict as JSON to MinIO.

        Returns the object path (e.g. '2025/03/abc123.json').
        """
        now = datetime.utcnow()
        safe_id = message_id.replace("/", "_").replace("<", "").replace(">", "")
        object_path = f"{now.year}/{now.month:02d}/{safe_id}.json"

        payload = json.dumps(email_dict, ensure_ascii=False, default=str).encode("utf-8")
        data = io.BytesIO(payload)

        try:
            self.client.put_object(
                BUCKET_EMAILS,
                object_path,
                data,
                length=len(payload),
                content_type="application/json",
            )
            logger.debug("Stored email %s at %s/%s", message_id, BUCKET_EMAILS, object_path)
        except S3Error as exc:
            logger.error("MinIO put_object failed for %s: %s", message_id, exc)
            raise

        return f"s3://{BUCKET_EMAILS}/{object_path}"

    def get_email(self, path: str) -> dict:
        """
        Retrieve an email dict from MinIO.

        Accepts either the full s3:// URI or a bare object path.
        """
        if path.startswith("s3://"):
            # Strip scheme and bucket prefix
            without_scheme = path[len("s3://"):]
            _, object_path = without_scheme.split("/", 1)
        else:
            object_path = path

        try:
            response = self.client.get_object(BUCKET_EMAILS, object_path)
            data = response.read()
            return json.loads(data.decode("utf-8"))
        except S3Error as exc:
            logger.error("MinIO get_object failed for %s: %s", path, exc)
            raise

    # ── Attachment storage ────────────────────────────────────────────────────

    def store_attachment(self, email_id: str, filename: str, data: bytes) -> str:
        """
        Store an attachment binary in MinIO.

        Returns the object path.
        """
        safe_name = filename.replace(" ", "_")
        object_path = f"{email_id}/{safe_name}"

        buf = io.BytesIO(data)
        try:
            self.client.put_object(
                BUCKET_ATTACHMENTS,
                object_path,
                buf,
                length=len(data),
            )
            logger.debug("Stored attachment %s at %s/%s", filename, BUCKET_ATTACHMENTS, object_path)
        except S3Error as exc:
            logger.error("MinIO put attachment failed for %s: %s", filename, exc)
            raise

        return f"s3://{BUCKET_ATTACHMENTS}/{object_path}"

    def presigned_url(self, bucket: str, object_path: str, expires_hours: int = 1) -> str:
        """Generate a presigned GET URL valid for *expires_hours* hours."""
        from datetime import timedelta
        url = self.client.presigned_get_object(
            bucket, object_path, expires=timedelta(hours=expires_hours)
        )
        return url


# Singleton helper -  lazily constructed
_storage: Optional[MinIOStorage] = None


def get_storage() -> MinIOStorage:
    global _storage
    if _storage is None:
        _storage = MinIOStorage()
    return _storage
