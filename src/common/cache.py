"""
Cache layer with Redis-first behavior and in-memory fallback.

Provides:
  - SHA-256 exact dedup for emails
  - SimHash near-dedup
  - MinHash near-dedup
  - Last-UID checkpoint per account
  - Pipeline state persistence

When Redis is unavailable in local development, the cache transparently falls
back to an in-memory store so the app can still operate end to end.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from typing import Optional

import redis as _redis_lib

logger = logging.getLogger(__name__)

_NS_SHA256 = "dedup:sha256:"
_NS_SIMHASH = "dedup:simhash:"
_NS_CHECKPOINT = "checkpoint:last_uid:"
_NS_PIPELINE = "pipeline:state"
_NS_MINHASH = "dedup:minhash:"

_DEFAULT_TTL_DAYS = 30


def _get_url() -> str:
    host = os.getenv("REDIS_HOST", "localhost")
    port = os.getenv("REDIS_PORT", "6379")
    password = os.getenv("REDIS_PASSWORD", "") or None
    if password:
        return f"redis://:{password}@{host}:{port}/0"
    return f"redis://{host}:{port}/0"


class RedisCache:
    """Redis-backed dedup, checkpoint, and pipeline-state store."""

    _MINHASH_NUM_PERM = 128

    def __init__(self):
        self._client: Optional[_redis_lib.Redis] = None
        self._memory_mode = False
        self._memory_store: dict[str, str] = {}

    @property
    def r(self) -> _redis_lib.Redis:
        if self._client is None:
            self._client = _redis_lib.from_url(
                _get_url(), decode_responses=True, socket_timeout=3
            )
        return self._client

    @property
    def backend_name(self) -> str:
        return "memory" if self._memory_mode else "redis"

    def _enable_memory_fallback(self, exc: Exception) -> None:
        if not self._memory_mode:
            logger.warning("Redis unavailable, switching to in-memory cache: %s", exc)
        self._memory_mode = True

    def _get(self, key: str) -> Optional[str]:
        if self._memory_mode:
            return self._memory_store.get(key)
        try:
            return self.r.get(key)
        except _redis_lib.RedisError as exc:
            self._enable_memory_fallback(exc)
            return self._memory_store.get(key)

    def _set(self, key: str, value: str) -> None:
        if self._memory_mode:
            self._memory_store[key] = value
            return
        try:
            self.r.set(key, value)
        except _redis_lib.RedisError as exc:
            self._enable_memory_fallback(exc)
            self._memory_store[key] = value

    def _setex(self, key: str, ttl_seconds: int, value: str) -> None:
        if self._memory_mode:
            self._memory_store[key] = value
            return
        try:
            self.r.setex(key, ttl_seconds, value)
        except _redis_lib.RedisError as exc:
            self._enable_memory_fallback(exc)
            self._memory_store[key] = value

    def _delete(self, key: str) -> None:
        if self._memory_mode:
            self._memory_store.pop(key, None)
            return
        try:
            self.r.delete(key)
        except _redis_lib.RedisError as exc:
            self._enable_memory_fallback(exc)
            self._memory_store.pop(key, None)

    def _exists(self, key: str) -> bool:
        if self._memory_mode:
            return key in self._memory_store
        try:
            return bool(self.r.exists(key))
        except _redis_lib.RedisError as exc:
            self._enable_memory_fallback(exc)
            return key in self._memory_store

    def _scan_keys(self, prefix: str) -> list[str]:
        pattern = f"{prefix}*"
        if self._memory_mode:
            return [k for k in self._memory_store if k.startswith(prefix)]
        try:
            return list(self.r.scan_iter(pattern))
        except _redis_lib.RedisError as exc:
            self._enable_memory_fallback(exc)
            return [k for k in self._memory_store if k.startswith(prefix)]

    def ping(self) -> bool:
        """Return True if Redis is reachable or memory fallback is active."""
        if self._memory_mode:
            return True
        try:
            self.r.ping()
            logger.info("Redis connection OK (%s)", _get_url())
            return True
        except _redis_lib.RedisError as exc:
            self._enable_memory_fallback(exc)
            return True

    @staticmethod
    def _email_sha256(message_id: str, sender: str, date: str, subject: str) -> str:
        raw = f"{message_id}|{sender}|{date}|{subject}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def is_duplicate_sha256(
        self,
        message_id: str,
        sender: str = "",
        date: str = "",
        subject: str = "",
        *,
        sha256: Optional[str] = None,
    ) -> bool:
        key = _NS_SHA256 + (sha256 or self._email_sha256(message_id, sender, date, subject))
        return self._exists(key)

    def mark_seen_sha256(
        self,
        message_id: str,
        sender: str = "",
        date: str = "",
        subject: str = "",
        *,
        sha256: Optional[str] = None,
        ttl_days: int = _DEFAULT_TTL_DAYS,
    ) -> None:
        key = _NS_SHA256 + (sha256 or self._email_sha256(message_id, sender, date, subject))
        self._setex(key, ttl_days * 86400, "1")

    @staticmethod
    def _simhash(text: str) -> int:
        v = [0] * 64
        words = text.lower().split()
        for word in words:
            h = int(hashlib.md5(word.encode()).hexdigest(), 16) & ((1 << 64) - 1)
            for i in range(64):
                v[i] += 1 if (h >> i) & 1 else -1
        result = 0
        for i in range(64):
            if v[i] > 0:
                result |= 1 << i
        return result

    @staticmethod
    def _hamming_distance(a: int, b: int) -> int:
        return bin(a ^ b).count("1")

    def is_duplicate_simhash(self, text: str, threshold: float = 0.92) -> bool:
        new_hash = self._simhash(text)
        max_distance = int((1 - threshold) * 64)
        for key in self._scan_keys(_NS_SIMHASH):
            stored = int(self._get(key) or "0")
            if self._hamming_distance(new_hash, stored) <= max_distance:
                return True
        return False

    def mark_seen_simhash(
        self, text: str, key_suffix: str, ttl_days: int = _DEFAULT_TTL_DAYS
    ) -> None:
        h = self._simhash(text)
        self._setex(f"{_NS_SIMHASH}{key_suffix}", ttl_days * 86400, str(h))

    def get_checkpoint(self, account_id: str) -> Optional[str]:
        return self._get(f"{_NS_CHECKPOINT}{account_id}")

    def set_checkpoint(self, account_id: str, uid: str) -> None:
        self._set(f"{_NS_CHECKPOINT}{account_id}", uid)

    def get_pipeline_state(self) -> dict:
        raw = self._get(_NS_PIPELINE)
        if raw:
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                pass
        return {}

    def set_pipeline_state(self, state: dict) -> None:
        self._set(_NS_PIPELINE, json.dumps(state, default=str))

    def clear_pipeline_state(self) -> None:
        self._delete(_NS_PIPELINE)

    @staticmethod
    def _shingles(text: str, k: int = 3) -> set[str]:
        t = text.lower()
        return {t[i: i + k] for i in range(len(t) - k + 1)} if len(t) >= k else {t}

    def is_duplicate_minhash(
        self, text: str, threshold: float = 0.85, key_suffix: str = ""
    ) -> bool:
        try:
            import json as _j

            new_sig = self._compute_minhash(text)
            for key in self._scan_keys(_NS_MINHASH):
                raw = self._get(key)
                if not raw:
                    continue
                stored_sig = _j.loads(raw)
                if self._jaccard_from_minhash(new_sig, stored_sig) >= threshold:
                    return True
            return False
        except Exception as exc:
            logger.debug("is_duplicate_minhash error: %s", exc)
            return False

    def mark_seen_minhash(
        self,
        text: str,
        key_suffix: str,
        ttl_days: int = _DEFAULT_TTL_DAYS,
    ) -> None:
        try:
            import json as _j

            sig = self._compute_minhash(text)
            self._setex(
                f"{_NS_MINHASH}{key_suffix}",
                ttl_days * 86400,
                _j.dumps(sig),
            )
        except Exception as exc:
            logger.debug("mark_seen_minhash error: %s", exc)

    def _compute_minhash(self, text: str) -> list[int]:
        shingles = self._shingles(text)
        try:
            from datasketch import MinHash

            mh = MinHash(num_perm=self._MINHASH_NUM_PERM)
            for s in shingles:
                mh.update(s.encode())
            return [int(v) for v in mh.hashvalues]
        except ImportError:
            sig = []
            for seed in range(self._MINHASH_NUM_PERM):
                min_h = float("inf")
                for s in shingles:
                    h = int(hashlib.md5(f"{seed}:{s}".encode()).hexdigest(), 16)
                    if h < min_h:
                        min_h = h
                sig.append(min_h if min_h != float("inf") else 0)
            return sig

    @staticmethod
    def _jaccard_from_minhash(sig_a: list[int], sig_b: list[int]) -> float:
        if not sig_a or not sig_b or len(sig_a) != len(sig_b):
            return 0.0
        matches = sum(1 for a, b in zip(sig_a, sig_b) if a == b)
        return matches / len(sig_a)


_cache: Optional[RedisCache] = None


def get_cache() -> RedisCache:
    global _cache
    if _cache is None:
        _cache = RedisCache()
    return _cache


def is_duplicate_sha256(sha256: str) -> bool:
    try:
        return get_cache().is_duplicate_sha256("", sha256=sha256)
    except Exception:
        return False


def mark_seen_sha256(sha256: str) -> None:
    try:
        get_cache().mark_seen_sha256("", sha256=sha256)
    except Exception:
        pass


def is_duplicate_minhash(text: str, threshold: float = 0.85) -> bool:
    try:
        return get_cache().is_duplicate_minhash(text, threshold)
    except Exception:
        return False


def mark_seen_minhash(text: str, key_suffix: str) -> None:
    try:
        get_cache().mark_seen_minhash(text, key_suffix)
    except Exception:
        pass
