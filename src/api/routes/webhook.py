"""
Webhook Endpoints — Inbound email push receivers.

Accepts and validates inbound email notifications from:
  - Generic SMTP relay (Mailgun, SendGrid, Postmark)
  - Microsoft Graph change notifications

Received emails are validated then dispatched to the Celery extraction queue.
If the Celery worker is unavailable the webhook still returns 200 (fail-open).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Header, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse

from src.common.config import get_settings
from src.common.observability import get_logger

logger = get_logger(__name__)
router = APIRouter()

_MSGRAPH_MSG_ID_RE = re.compile(r"Messages/([^/]+)$", re.IGNORECASE)


# ── HMAC signature validation ────────────────────────────────────────────────

def _verify_hmac(body: bytes, signature: str | None, secret: str) -> bool:
    """Validate HMAC-SHA256 signature (sha256=<hex> format)."""
    if not signature or not secret:
        return not secret  # dev mode: allow all if no secret configured
    try:
        _algo, received_hex = signature.split("=", 1)
    except ValueError:
        return False
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, received_hex)


def _dispatch_to_queue(message_id: str, institution_id: str | None = None) -> bool:
    """
    Dispatch a message to the Celery extraction queue.
    Returns False (and logs a warning) if Celery worker is unavailable.
    """
    try:
        from src.tasks.ingestion import ingest_single_email
        ingest_single_email.apply_async(
            kwargs={"message_id": message_id, "institution_id": institution_id},
            queue="email_ingestion",
        )
        logger.info("webhook_dispatched_to_queue", message_id=message_id)
        return True
    except Exception as exc:
        logger.warning("webhook_queue_dispatch_failed", message_id=message_id, error=str(exc))
        return False


# ── 1. Generic SMTP relay webhook ────────────────────────────────────────────

@router.post("/email", summary="Inbound SMTP relay webhook")
async def inbound_email_webhook(
    request: Request,
    x_webhook_signature: str | None = Header(default=None, alias="X-Webhook-Signature"),
    x_mailgun_signature: str | None = Header(default=None, alias="X-Mailgun-Signature"),
) -> dict[str, str]:
    """Receive raw MIME email from an SMTP relay (Mailgun, SendGrid, Postmark, MTA)."""
    settings = get_settings()
    body = await request.body()
    sig = x_webhook_signature or x_mailgun_signature

    if not _verify_hmac(body, sig, settings.webhook.secret_token):
        logger.warning("webhook_invalid_signature", content_length=len(body))
        raise HTTPException(status_code=401, detail="Invalid webhook signature")

    # Extract Message-ID from MIME headers
    message_id: str | None = None
    try:
        import email as _email_lib
        msg = _email_lib.message_from_bytes(body)
        message_id = (msg.get("Message-ID") or "").strip("<> ")
    except Exception:
        pass

    if not message_id:
        # Fallback: hash the body
        message_id = hashlib.sha256(body).hexdigest()

    logger.info("webhook_email_received", size_bytes=len(body), message_id=message_id)
    queued = _dispatch_to_queue(message_id)
    return {"status": "accepted", "size_bytes": str(len(body)), "queued": str(queued)}


# ── 2. Microsoft Graph change notification ───────────────────────────────────

@router.post("/msgraph", summary="Microsoft Graph change notification")
async def msgraph_notification(
    request: Request,
    background_tasks: BackgroundTasks,
    validationToken: str | None = Query(default=None),
) -> Any:
    """Handle MS Graph change notifications for Exchange/Office 365 inboxes."""
    # Subscription validation handshake — must echo token within 10 s
    if validationToken:
        logger.info("msgraph_subscription_validated")
        return PlainTextResponse(content=validationToken, status_code=200)

    body_bytes = await request.body()
    try:
        payload = json.loads(body_bytes)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    queued_count = 0
    for notification in payload.get("value", []):
        resource = notification.get("resource", "")
        logger.info("msgraph_notification_received", resource=resource)

        # Extract message ID from resource path e.g. "Users/{id}/Messages/{msgId}"
        m = _MSGRAPH_MSG_ID_RE.search(resource)
        if m:
            message_id = m.group(1)
            if _dispatch_to_queue(message_id):
                queued_count += 1
        else:
            logger.warning("msgraph_no_message_id", resource=resource)

    return {"status": "accepted", "notifications": len(payload.get("value", [])), "queued": queued_count}
