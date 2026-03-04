"""
Elasticsearch client — AcadExtract.

Provides a thin, graceful wrapper around the `elasticsearch` package.
All functions degrade gracefully when ES is unavailable.
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

_ES_URL  = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
_ES_USER = os.getenv("ELASTICSEARCH_USER", "")
_ES_PASS = os.getenv("ELASTICSEARCH_PASSWORD", "")

_client: Any = None


def es_client():
    """Return a cached Elasticsearch client (lazy init)."""
    global _client
    if _client is not None:
        return _client
    try:
        from elasticsearch import Elasticsearch

        kwargs: dict = {"hosts": [_ES_URL]}
        if _ES_USER:
            kwargs["http_auth"] = (_ES_USER, _ES_PASS)

        _client = Elasticsearch(**kwargs)
        _client.info()  # test connectivity
        logger.info("Elasticsearch connected: %s", _ES_URL)
    except Exception as exc:
        logger.warning("Elasticsearch unavailable (%s) — search degraded", exc)
        _client = _NullES()
    return _client


def search_students(query: str, size: int = 20) -> list[dict]:
    """
    Full-text search across student USN and name fields.
    Returns list of student dicts.  Returns [] when ES unavailable.
    """
    try:
        resp = es_client().search(
            index="students",
            body={
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": ["usn^2", "name"],
                        "fuzziness": "AUTO",
                    }
                },
                "size": size,
            },
        )
        return [hit["_source"] for hit in resp["hits"]["hits"]]
    except Exception as exc:
        logger.debug("search_students failed: %s", exc)
        return []


def bulk_index(client: Any, index: str, docs: list[dict]) -> int:
    """
    Bulk-index documents.  Returns count of successfully indexed docs.
    """
    if not docs:
        return 0
    try:
        from elasticsearch.helpers import bulk

        actions = [
            {
                "_index": index,
                "_id":    doc.get("id"),
                "_source": {k: v for k, v in doc.items() if k != "id"},
            }
            for doc in docs
        ]
        success, _ = bulk(client, actions, raise_on_error=False)
        return success
    except Exception as exc:
        logger.warning("bulk_index failed: %s", exc)
        return 0


def ensure_index(index: str, mappings: dict | None = None) -> None:
    """Create index if it does not exist."""
    try:
        client = es_client()
        if isinstance(client, _NullES):
            return
        if not client.indices.exists(index=index):
            body: dict = {}
            if mappings:
                body["mappings"] = mappings
            client.indices.create(index=index, body=body)
            logger.info("Created Elasticsearch index: %s", index)
    except Exception as exc:
        logger.debug("ensure_index(%s) failed: %s", index, exc)


# ── Null client for graceful degradation ─────────────────────────────────────

class _NullES:
    """Drop-in replacement when Elasticsearch is not available."""

    def index(self, **_: Any) -> None:            # type: ignore[override]
        pass

    def search(self, **_: Any) -> dict:           # type: ignore[override]
        return {"hits": {"hits": []}}

    def indices(self) -> "_NullIndices":          # type: ignore[override]
        return _NullIndices()

    def info(self) -> dict:                       # type: ignore[override]
        raise ConnectionError("ES null client")

    class _NullIndices:
        def exists(self, **_: Any) -> bool:
            return False

        def create(self, **_: Any) -> None:
            pass

    indices = _NullIndices()
