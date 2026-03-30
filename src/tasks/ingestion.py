"""
Celery tasks — Gmail ingestion.
Queue: email_ingestion
"""

from __future__ import annotations

import logging
from typing import Any

from src.common.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(
    name="tasks.sync_gmail_inbox",
    bind=True,
    max_retries=3,
    default_retry_delay=60,
    queue="email_ingestion",
)
def sync_gmail_inbox(self, institution_id: str | None = None) -> dict:
    """
    Periodic task: pull new emails from Gmail and push them through the
    classification/extraction pipeline.

    Beat schedule: every 15 minutes (configured in celery_app.py).
    """
    try:
        from src.common.cache import is_duplicate_sha256, mark_seen_sha256
        from src.tasks.extraction import extract_email
        import hashlib, json, os

        cache_path = "data/emails_cache.json"
        if not os.path.exists(cache_path):
            return {"status": "no_cache", "processed": 0}

        with open(cache_path) as f:
            emails: list[dict] = json.load(f)

        enqueued = 0
        for email in emails:
            body = email.get("body", "")
            h = hashlib.sha256(body.encode()).hexdigest()
            if is_duplicate_sha256(h):
                continue
            mark_seen_sha256(h)
            extract_email.apply_async(
                kwargs={"email": email, "institution_id": institution_id},
                queue="extraction",
            )
            enqueued += 1

        logger.info("sync_gmail_inbox: enqueued %d emails for extraction", enqueued)
        return {"status": "ok", "enqueued": enqueued, "total": len(emails)}

    except Exception as exc:
        logger.error("sync_gmail_inbox failed: %s", exc)
        raise self.retry(exc=exc)


@celery_app.task(
    name="tasks.ingest_single_email",
    bind=True,
    max_retries=2,
    queue="email_ingestion",
)
def ingest_single_email(self, message_id: str, institution_id: str | None = None) -> dict:
    """
    On-demand task: fetch a single Gmail message and enqueue for extraction.
    Called from the webhook handler when Gmail push notification arrives.
    """
    try:
        import hashlib
        from src.common.cache import is_duplicate_sha256, mark_seen_sha256
        from src.tasks.extraction import extract_email

        # Hash the message_id so dedup key is a proper SHA-256 hex digest
        msg_hash = hashlib.sha256(message_id.encode()).hexdigest()
        if is_duplicate_sha256(msg_hash):
            return {"status": "duplicate", "message_id": message_id}
        mark_seen_sha256(msg_hash)

        # Delegate to extraction immediately
        extract_email.apply_async(
            kwargs={"email": {"id": message_id}, "institution_id": institution_id},
            queue="extraction",
        )
        return {"status": "enqueued", "message_id": message_id}

    except Exception as exc:
        logger.error("ingest_single_email failed: %s", exc)
        raise self.retry(exc=exc)


@celery_app.task(
    name="tasks.sync_imap_inbox",
    bind=True,
    max_retries=3,
    default_retry_delay=120,
    queue="email_ingestion",
)
def sync_imap_inbox(
    self,
    account_id: str,
    institution_id: str | None = None,
) -> dict:
    """
    Periodic task: connect to an IMAP server and pull unread/recent emails.

    Loads account credentials from data/accounts.json (stored by accounts.py).
    Applies SHA-256 dedup on message bodies before enqueueing.
    """
    try:
        import json, hashlib, os, email as email_lib
        from email import policy as email_policy
        from src.common.cache import is_duplicate_sha256, mark_seen_sha256
        from src.tasks.extraction import extract_email

        accounts_path = "data/accounts.json"
        if not os.path.exists(accounts_path):
            return {"status": "no_accounts"}

        accounts = json.loads(open(accounts_path).read())
        account = next((a for a in accounts if a.get("id") == account_id), None)
        if not account:
            return {"status": "account_not_found", "account_id": account_id}

        host     = account.get("imap_host", "")
        port     = int(account.get("imap_port", 993))
        use_ssl  = account.get("imap_use_ssl", True)
        username = account.get("imap_username", "")
        password = account.get("imap_password", "")
        mailbox  = account.get("imap_mailbox", "INBOX")

        if not host or not username:
            return {"status": "incomplete_config"}

        import imapclient
        server = imapclient.IMAPClient(host, port=port, ssl=use_ssl)
        server.login(username, password)
        server.select_folder(mailbox, readonly=True)

        # Fetch messages from the last 7 days not yet seen
        import datetime
        since_date = (datetime.date.today() - datetime.timedelta(days=7))
        msg_ids = server.search(["SINCE", since_date])
        logger.info("sync_imap_inbox: found %d messages in %s", len(msg_ids), mailbox)

        enqueued = 0
        for chunk_start in range(0, len(msg_ids), 20):
            chunk = msg_ids[chunk_start:chunk_start + 20]
            fetched = server.fetch(chunk, ["RFC822", "ENVELOPE"])
            for uid, data in fetched.items():
                raw_bytes = data.get(b"RFC822", b"")
                if not raw_bytes:
                    continue

                # Parse MIME
                msg = email_lib.message_from_bytes(raw_bytes, policy=email_policy.default)
                body = ""
                if msg.is_multipart():
                    for part in msg.walk():
                        ct = part.get_content_type()
                        if ct == "text/plain":
                            body = part.get_content() or ""
                            break
                else:
                    body = msg.get_content() or ""

                subject = str(msg.get("Subject", ""))
                from_   = str(msg.get("From", ""))
                date_   = str(msg.get("Date", ""))
                msg_id  = str(msg.get("Message-ID", "")).strip("<>")
                in_reply_to = str(msg.get("In-Reply-To", "")).strip("<>")
                references_raw = str(msg.get("References", ""))
                references = [r.strip("<>") for r in references_raw.split() if r.strip("<>")]

                # SHA-256 dedup on body content
                body_hash = hashlib.sha256(body.encode()).hexdigest()
                if is_duplicate_sha256(body_hash):
                    continue
                mark_seen_sha256(body_hash)

                email_dict = {
                    "id":           msg_id or body_hash[:16],
                    "message_id":   msg_id,
                    "in_reply_to":  in_reply_to,
                    "references":   references,
                    "thread_depth": len(references) if references else (1 if in_reply_to else 0),
                    "subject":      subject,
                    "from":         from_,
                    "date":         date_,
                    "body":         body[:2000],
                    "attachments":  [],
                    "source":       "imap",
                    "account_id":   account_id,
                }

                extract_email.apply_async(
                    kwargs={"email": email_dict, "institution_id": institution_id},
                    queue="extraction",
                )
                enqueued += 1

        server.logout()
        logger.info("sync_imap_inbox: enqueued %d emails from %s", enqueued, account_id)
        return {"status": "ok", "enqueued": enqueued, "account_id": account_id}

    except Exception as exc:
        logger.error("sync_imap_inbox failed for %s: %s", account_id, exc)
        raise self.retry(exc=exc)
