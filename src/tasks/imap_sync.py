"""
Celery task — IMAP inbox polling.
Queue: email_ingestion

Polls every IMAP account stored in accounts.json,
fetches unseen messages since the last checkpoint,
deduplicates via SHA-256 + SimHash, and enqueues each
email for extraction via extract_email.
"""

from __future__ import annotations

import email as _email_lib
import hashlib
import json
import logging
import os
from email.header import decode_header as _decode_header
from pathlib import Path
from typing import Any

from src.common.celery_app import celery_app

logger = logging.getLogger(__name__)

PROJECT_ROOT   = Path(__file__).resolve().parents[2]
ACCOUNTS_FILE  = PROJECT_ROOT / "config" / "secrets" / "accounts.json"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _decode_str(raw: Any) -> str:
    """Decode a possibly-encoded email header value to plain str."""
    if raw is None:
        return ""
    if isinstance(raw, str):
        return raw
    parts = _decode_header(raw)
    decoded = []
    for part, charset in parts:
        if isinstance(part, bytes):
            decoded.append(part.decode(charset or "utf-8", errors="replace"))
        else:
            decoded.append(str(part))
    return " ".join(decoded)


def _get_text_body(msg: Any) -> str:
    """Extract plain-text body from an email.message.Message object."""
    body_parts: list[str] = []
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                try:
                    charset = part.get_content_charset() or "utf-8"
                    body_parts.append(
                        part.get_payload(decode=True).decode(charset, errors="replace")
                    )
                except Exception:
                    pass
    else:
        try:
            charset = msg.get_content_charset() or "utf-8"
            body_parts.append(
                msg.get_payload(decode=True).decode(charset, errors="replace")
            )
        except Exception:
            pass
    return "\n".join(body_parts)[:4000]


def _load_accounts() -> list[dict]:
    if not ACCOUNTS_FILE.exists():
        return []
    try:
        return json.loads(ACCOUNTS_FILE.read_text())
    except Exception:
        return []


# ── Celery task ───────────────────────────────────────────────────────────────

@celery_app.task(
    name="tasks.sync_imap_accounts",
    bind=True,
    max_retries=2,
    default_retry_delay=120,
    queue="email_ingestion",
)
def sync_imap_accounts(self, institution_id: str | None = None) -> dict:
    """
    Poll every IMAP account in accounts.json for unseen messages.
    Beat schedule: every 10 minutes (configured in celery_app.py).

    For each new message:
      1. Fetch full MIME message
      2. SHA-256 dedup on body hash
      3. SimHash near-dup check
      4. Enqueue for extraction via extract_email.apply_async()
    """
    try:
        from src.common.cache import get_cache, mark_seen_sha256
        from src.tasks.extraction import extract_email

        accounts = [a for a in _load_accounts() if a.get("type") == "imap"]
        if not accounts:
            return {"status": "no_imap_accounts", "enqueued": 0}

        cache = get_cache()
        total_enqueued = 0

        for account in accounts:
            account_id = account.get("id", "unknown")
            host    = account.get("config", {}).get("host", "")
            port    = int(account.get("config", {}).get("port", 993))
            use_ssl = account.get("config", {}).get("ssl", True)
            username  = account.get("config", {}).get("username", "")
            password  = account.get("config", {}).get("password", "")
            mailbox = account.get("config", {}).get("mailbox", "INBOX")

            if not host or not username:
                continue

            try:
                import imapclient  # type: ignore
                server = imapclient.IMAPClient(host, port=port, ssl=use_ssl, timeout=30)
                server.login(username, password)
                server.select_folder(mailbox, readonly=True)

                # Fetch only unseen messages since the last checkpoint UID
                last_uid = cache.get_checkpoint(account_id)
                if last_uid:
                    uids = server.search(["UID", f"{int(last_uid)+1}:*", "UNSEEN"])
                else:
                    uids = server.search(["UNSEEN"])

                if not uids:
                    server.logout()
                    logger.info("imap_sync: no new messages for account %s", account_id)
                    continue

                # Fetch RFC822 (full message) in batches of 20
                batch_size = 20
                enqueued_this_account = 0
                max_uid_seen = int(last_uid or 0)

                for i in range(0, len(uids), batch_size):
                    batch = uids[i : i + batch_size]
                    fetch_resp = server.fetch(batch, ["RFC822", "ENVELOPE"])

                    for uid, data in fetch_resp.items():
                        try:
                            raw_bytes: bytes = data.get(b"RFC822", b"")
                            if not raw_bytes:
                                continue

                            msg = _email_lib.message_from_bytes(raw_bytes)
                            body = _get_text_body(msg)

                            # SHA-256 dedup
                            body_hash = hashlib.sha256(body.encode()).hexdigest()
                            if cache.is_duplicate_sha256("", sha256=body_hash):
                                continue

                            # SimHash near-dup check
                            if body and cache.is_duplicate_simhash(body):
                                continue

                            mark_seen_sha256(body_hash)
                            cache.mark_seen_simhash(body, key_suffix=body_hash[:16])

                            # Build email dict
                            subject = _decode_str(msg.get("Subject", ""))
                            sender  = _decode_str(msg.get("From", ""))
                            msg_id  = _decode_str(msg.get("Message-ID", "")).strip("<>")
                            date_str = _decode_str(msg.get("Date", ""))

                            email_dict = {
                                "id":         msg_id or f"imap:{account_id}:{uid}",
                                "subject":    subject,
                                "from":       sender,
                                "date":       date_str,
                                "body":       body,
                                "source":     "imap",
                                "account_id": account_id,
                            }

                            extract_email.apply_async(
                                kwargs={"email": email_dict, "institution_id": institution_id},
                                queue="extraction",
                            )
                            enqueued_this_account += 1
                            max_uid_seen = max(max_uid_seen, int(uid))

                        except Exception as exc:
                            logger.warning("imap_sync: error processing uid %s: %s", uid, exc)

                # Save checkpoint so next run picks up from here
                if max_uid_seen > int(last_uid or 0):
                    cache.set_checkpoint(account_id, str(max_uid_seen))

                server.logout()
                logger.info(
                    "imap_sync: account=%s enqueued=%d",
                    account_id, enqueued_this_account,
                )
                total_enqueued += enqueued_this_account

            except Exception as exc:
                logger.error("imap_sync: failed for account %s: %s", account_id, exc)
                continue

        return {"status": "ok", "accounts": len(accounts), "enqueued": total_enqueued}

    except Exception as exc:
        logger.error("sync_imap_accounts failed: %s", exc)
        raise self.retry(exc=exc)
