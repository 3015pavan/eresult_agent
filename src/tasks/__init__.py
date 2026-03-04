"""Celery tasks package."""
from .ingestion   import sync_gmail_inbox, ingest_single_email
from .extraction  import extract_email, extract_attachment
from .indexing    import index_student, rebuild_all_embeddings, refresh_elasticsearch

__all__ = [
    "sync_gmail_inbox",
    "ingest_single_email",
    "extract_email",
    "extract_attachment",
    "index_student",
    "rebuild_all_embeddings",
    "refresh_elasticsearch",
]
