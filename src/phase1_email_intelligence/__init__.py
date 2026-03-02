"""Phase 1 — Email Intelligence Pipeline."""
from .ingestion import EmailIngestionService
from .classification import EmailClassifier
from .dedup import DeduplicationEngine
from .attachment_extractor import AttachmentExtractor

__all__ = [
    "EmailIngestionService",
    "EmailClassifier",
    "DeduplicationEngine",
    "AttachmentExtractor",
]
