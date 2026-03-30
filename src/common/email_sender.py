"""
Outbound email sender for AcadExtract.

Supports two backends (tried in order):
  1. Gmail API (sendMessage) — used when config/secrets/token.json present
  2. SMTP — uses SMTP_HOST / SMTP_PORT / SMTP_USER / SMTP_PASSWORD env vars

All public functions are best-effort: they log errors and return False on failure
rather than raising, so a send failure never blocks the extraction pipeline.
"""

from __future__ import annotations

import base64
import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_TOKEN_PATH = _PROJECT_ROOT / "config" / "secrets" / "token.json"


# ── Gmail API sender ──────────────────────────────────────────────────────────

def _send_via_gmail_api(
    to: str,
    subject: str,
    body_text: str,
    body_html: Optional[str] = None,
    reply_to_message_id: Optional[str] = None,
    thread_id: Optional[str] = None,
) -> bool:
    """Send email via Gmail API using stored OAuth token."""
    try:
        from google.oauth2.credentials import Credentials  # type: ignore
        from googleapiclient.discovery import build  # type: ignore

        if not _TOKEN_PATH.exists():
            logger.error("Gmail API token not found at %s", _TOKEN_PATH)
            return False

        creds = Credentials.from_authorized_user_file(str(_TOKEN_PATH))
        service = build("gmail", "v1", credentials=creds, cache_discovery=False)

        msg = MIMEMultipart("alternative")
        msg["To"] = to
        msg["Subject"] = subject
        if reply_to_message_id:
            msg["In-Reply-To"] = f"<{reply_to_message_id.strip('<>')}>"
            msg["References"] = f"<{reply_to_message_id.strip('<>')}>"

        msg.attach(MIMEText(body_text, "plain"))
        if body_html:
            msg.attach(MIMEText(body_html, "html"))

        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        body: dict = {"raw": raw}
        if thread_id:
            body["threadId"] = thread_id

        service.users().messages().send(userId="me", body=body).execute()
        logger.info("email_sender: sent via Gmail API to %s", to)
        return True

    except Exception as exc:
        logger.error("email_sender: gmail_api failed: %s", exc)
        return False


# ── SMTP sender ───────────────────────────────────────────────────────────────

def _send_via_smtp(
    to: str,
    subject: str,
    body_text: str,
    body_html: Optional[str] = None,
    reply_to_message_id: Optional[str] = None,
) -> bool:
    """Send email via SMTP using SMTPConfig."""
    try:
        from src.common.config import get_settings
        smtp = get_settings().smtp

        if not smtp.configured:
            logger.debug("email_sender: SMTP not configured (no user/password)")
            try:
                from src.common.config import get_settings
                smtp = get_settings().smtp

                if not smtp.configured:
                    logger.error("email_sender: SMTP not configured (no user/password)")
                    return False

                msg = MIMEMultipart("alternative")
                msg["From"] = f"{smtp.from_name} <{smtp.user}>"
                msg["To"] = to
                msg["Subject"] = subject
                if reply_to_message_id:
                    clean_mid = reply_to_message_id.strip("<>")
                    msg["In-Reply-To"] = f"<{clean_mid}>"
                    msg["References"] = f"<{clean_mid}>"

                msg.attach(MIMEText(body_text, "plain"))
                if body_html:
                    msg.attach(MIMEText(body_html, "html"))

                server = smtplib.SMTP(smtp.host, smtp.port)
                server.starttls()
                server.login(smtp.user, smtp.password)
                server.sendmail(smtp.user, to, msg.as_string())
                server.quit()
                logger.info("email_sender: sent via SMTP to %s", to)
                return True

            except Exception as exc:
                logger.error("email_sender: smtp failed: %s", exc)
                return False

# ── Public API ────────────────────────────────────────────────────────────────

def send_reply(
    to: str,
    subject: str,
    body_text: str,
    body_html: Optional[str] = None,
    reply_to_message_id: Optional[str] = None,
    thread_id: Optional[str] = None,
) -> bool:
    """
    Send a reply email.  Tries Gmail API first, falls back to SMTP.

    Args:
        to:                   Recipient email address.
        subject:              Email subject (prefix with "Re: " for replies).
        body_text:            Plain-text body.
        body_html:            Optional HTML body.
        reply_to_message_id:  Original Message-ID for threading headers.
        thread_id:            Gmail thread ID (optional; used by Gmail API only).

    Returns:
        True if sent, False if all backends failed.
    """
    if not to or "@" not in to:
        logger.debug("email_sender: invalid or missing recipient '%s'", to)
        return False

    if _send_via_gmail_api(to, subject, body_text, body_html, reply_to_message_id, thread_id):
        return True
    return _send_via_smtp(to, subject, body_text, body_html, reply_to_message_id)


def send_extraction_confirmation(
    sender_email: str,
    original_subject: str,
    records_saved: int,
    reply_to_message_id: Optional[str] = None,
    thread_id: Optional[str] = None,
) -> bool:
    """
    Send a standard acknowledgement reply after successful result extraction.

    Args:
        sender_email:         Email address to reply to.
        original_subject:     Subject of the original email.
        records_saved:        Number of student subject records saved to DB.
        reply_to_message_id:  Original Message-ID for threading.
        thread_id:            Gmail thread ID.

    Returns:
        True if sent successfully.
    """
    subject = f"Re: {original_subject}" if original_subject else "Re: Result Submission Received"

    body_text = (
        f"Dear Sender,\n\n"
        f"Thank you for submitting the student results.\n\n"
        f"We have successfully processed your email and extracted "
        f"{records_saved} result record(s) into the AcadExtract database.\n\n"
        f"If you believe any data was missed or incorrect, please contact the admin.\n\n"
        f"Regards,\nAcadExtract System"
    )

    body_html = (
        "<p>Dear Sender,</p>"
        "<p>Thank you for submitting the student results.</p>"
        "<p>We have successfully processed your email and extracted "
        f"<strong>{records_saved} result record(s)</strong> into the AcadExtract database.</p>"
        "<p>If you believe any data was missed or incorrect, please contact the admin.</p>"
        "<p>Regards,<br><strong>AcadExtract System</strong></p>"
    )

    return send_reply(
        to=sender_email,
        subject=subject,
        body_text=body_text,
        body_html=body_html,
        reply_to_message_id=reply_to_message_id,
        thread_id=thread_id,
    )
