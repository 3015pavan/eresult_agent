"""
Webhook Endpoints — Inbound email push receivers.

Accepts and validates inbound email notifications from:
  - Generic SMTP relay (Mailgun, SendGrid, Postmark)
  - Microsoft Graph change notifications

Received emails are logged. Integrate an email queue consumer here
to enable automated processing of inbound webhooks.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Header, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse

from src.common.config import get_settings
from src.common.observability import get_logger

logger = get_logger(__name__)
router = APIRouter()


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

    logger.info("webhook_email_received", size_bytes=len(body))
    # TODO: push to processing queue when a consumer is available
    return {"status": "accepted", "size_bytes": len(body)}


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

    for notification in payload.get("value", []):
        resource = notification.get("resource", "")
        logger.info("msgraph_notification_received", resource=resource)
        # TODO: fetch and queue message when a consumer is available

    return {"status": "accepted"}
