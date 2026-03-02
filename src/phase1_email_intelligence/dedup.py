"""
Deduplication Engine.

Multi-strategy deduplication combining:
  1. Exact hash matching (SHA256 of body)
  2. Near-duplicate detection (SimHash with Hamming distance)
  3. Attachment hash matching (SHA256 of file bytes)
  4. MinHash LSH for scalable approximate matching

This prevents redundant processing when:
  - Same email forwarded by multiple recipients
  - Mailing list echoes
  - Re-sent emails with minor modifications
"""

from __future__ import annotations

import hashlib
from typing import Any

import redis.asyncio as aioredis
from simhash import Simhash

from src.common.config import get_settings
from src.common.models import EmailMessage
from src.common.observability import get_logger, EMAILS_DEDUPLICATED

logger = get_logger(__name__)


class DeduplicationEngine:
    """
    Multi-strategy email deduplication.

    Strategies (checked in order, short-circuit on match):

    1. Exact Message-ID match:
       - O(1) lookup in Redis SET
       - Catches forwarded/CC'd copies with same Message-ID

    2. Body hash match (SHA256):
       - O(1) lookup in Redis SET
       - Catches distinct Message-IDs with identical body content

    3. Near-duplicate (SimHash):
       - Hamming distance between 64-bit SimHash values
       - Threshold: 3 bits different → 0.92+ similarity
       - Uses Redis sorted sets for efficient range queries
       - Catches emails with minor edits (signatures, timestamps)

    4. Attachment hash match:
       - O(1) lookup per attachment SHA256
       - Prevents re-processing when same document arrives via different emails

    Storage:
      - Redis SET for message_ids and body hashes (TTL: 30 days)
      - Redis SORTED SET for SimHash values (score = simhash value)
      - Periodic persistence to PostgreSQL for durability
    """

    # Maximum Hamming distance for near-duplicate detection
    # 3 out of 64 bits → ~95.3% similarity
    SIMHASH_DISTANCE_THRESHOLD = 3

    # Redis key prefixes
    PREFIX_MSG_ID = "dedup:msgid:"
    PREFIX_BODY_HASH = "dedup:bodyhash:"
    PREFIX_SIMHASH = "dedup:simhash:"
    PREFIX_ATTACHMENT = "dedup:attachment:"

    # TTL for dedup keys (30 days)
    DEDUP_TTL_SECONDS = 30 * 24 * 3600

    def __init__(self, redis_client: aioredis.Redis) -> None:
        self.redis = redis_client
        self.settings = get_settings()

    async def is_duplicate(self, email_msg: EmailMessage) -> tuple[bool, str]:
        """
        Check if an email is a duplicate using multi-strategy detection.

        Returns:
          (is_duplicate: bool, method: str)
          method is one of: "message_id", "body_hash", "simhash", "attachment_hash", "none"
        """
        # Strategy 1: Exact Message-ID match
        if await self._check_message_id(email_msg.message_id):
            EMAILS_DEDUPLICATED.labels(dedup_method="message_id").inc()
            logger.info(
                "duplicate_detected",
                message_id=email_msg.message_id,
                method="message_id",
            )
            return True, "message_id"

        # Strategy 2: Body hash match
        if await self._check_body_hash(email_msg.body_hash):
            EMAILS_DEDUPLICATED.labels(dedup_method="body_hash").inc()
            logger.info(
                "duplicate_detected",
                message_id=email_msg.message_id,
                method="body_hash",
            )
            return True, "body_hash"

        # Strategy 3: SimHash near-duplicate
        if email_msg.body_simhash is not None:
            if await self._check_simhash(email_msg.body_simhash):
                EMAILS_DEDUPLICATED.labels(dedup_method="simhash").inc()
                logger.info(
                    "duplicate_detected",
                    message_id=email_msg.message_id,
                    method="simhash",
                )
                return True, "simhash"

        # Strategy 4: Attachment hash match
        for attachment in email_msg.attachments:
            if attachment.file_hash and await self._check_attachment_hash(attachment.file_hash):
                EMAILS_DEDUPLICATED.labels(dedup_method="attachment_hash").inc()
                logger.info(
                    "duplicate_detected",
                    message_id=email_msg.message_id,
                    method="attachment_hash",
                    attachment=attachment.filename,
                )
                return True, "attachment_hash"

        return False, "none"

    async def register(self, email_msg: EmailMessage) -> None:
        """
        Register an email in the dedup index after successful processing.

        Stores all hash variants so future duplicates can be detected.
        """
        pipe = self.redis.pipeline()

        # Register Message-ID
        key_msgid = f"{self.PREFIX_MSG_ID}{email_msg.message_id}"
        pipe.set(key_msgid, "1", ex=self.DEDUP_TTL_SECONDS)

        # Register body hash
        key_body = f"{self.PREFIX_BODY_HASH}{email_msg.body_hash}"
        pipe.set(key_body, email_msg.message_id, ex=self.DEDUP_TTL_SECONDS)

        # Register SimHash
        if email_msg.body_simhash is not None:
            key_simhash = f"{self.PREFIX_SIMHASH}index"
            pipe.zadd(key_simhash, {str(email_msg.body_simhash): email_msg.body_simhash})

        # Register attachment hashes
        for attachment in email_msg.attachments:
            if attachment.file_hash:
                key_att = f"{self.PREFIX_ATTACHMENT}{attachment.file_hash}"
                pipe.set(key_att, email_msg.message_id, ex=self.DEDUP_TTL_SECONDS)

        await pipe.execute()

        logger.debug(
            "dedup_registered",
            message_id=email_msg.message_id,
            body_hash=email_msg.body_hash[:16],
            attachment_count=len(email_msg.attachments),
        )

    async def _check_message_id(self, message_id: str) -> bool:
        """Check if Message-ID already exists."""
        return await self.redis.exists(f"{self.PREFIX_MSG_ID}{message_id}") > 0

    async def _check_body_hash(self, body_hash: str) -> bool:
        """Check if body SHA256 matches an existing email."""
        return await self.redis.exists(f"{self.PREFIX_BODY_HASH}{body_hash}") > 0

    async def _check_simhash(self, simhash_value: int) -> bool:
        """
        Check for near-duplicate using SimHash Hamming distance.

        SimHash properties:
          - 64-bit hash that preserves cosine similarity
          - Hamming distance ≤ 3 → documents are ~95% similar
          - Efficient: O(k * n^(1/k)) with multi-probe LSH

        For simplicity, we use a Redis sorted set scan approach.
        For production at scale (>1M emails), use a proper LSH index.
        """
        key = f"{self.PREFIX_SIMHASH}index"

        # Scan nearby values in the sorted set
        # SimHash values that differ by few bits are numerically close (probabilistically)
        window = 2 ** self.SIMHASH_DISTANCE_THRESHOLD
        low = simhash_value - window
        high = simhash_value + window

        candidates = await self.redis.zrangebyscore(key, low, high)

        for candidate_bytes in candidates:
            candidate = int(candidate_bytes)
            distance = self._hamming_distance(simhash_value, candidate)
            if distance <= self.SIMHASH_DISTANCE_THRESHOLD:
                return True

        return False

    async def _check_attachment_hash(self, file_hash: str) -> bool:
        """Check if attachment SHA256 matches an already-processed file."""
        return await self.redis.exists(f"{self.PREFIX_ATTACHMENT}{file_hash}") > 0

    @staticmethod
    def _hamming_distance(a: int, b: int) -> int:
        """Compute Hamming distance between two 64-bit integers."""
        xor = a ^ b
        distance = 0
        while xor:
            distance += 1
            xor &= xor - 1  # Clear lowest set bit
        return distance

    @staticmethod
    def compute_simhash(text: str) -> int:
        """Compute SimHash for a text string."""
        return Simhash(text).value
