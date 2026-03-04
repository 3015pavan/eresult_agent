"""
Email Account Management Routes.

Stores configured email accounts in data/accounts.json.
Supports: gmail, imap, msgraph, webhook.

Endpoints:
  GET    /api/v1/accounts                    — list all accounts
  POST   /api/v1/accounts                    — add/update an account
  DELETE /api/v1/accounts/{account_id}       — remove an account
  POST   /api/v1/accounts/{account_id}/test  — live connection test
"""

from __future__ import annotations

import json
import ssl
import uuid
from pathlib import Path
from typing import Any, Literal

import aiohttp
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.common.observability import get_logger

logger = get_logger(__name__)
router = APIRouter()

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
ACCOUNTS_FILE = _PROJECT_ROOT / "data" / "accounts.json"

ProviderType = Literal["gmail", "imap", "msgraph", "webhook"]


# ── Storage helpers ──────────────────────────────────────────────────


def _load_accounts() -> list[dict]:
    if not ACCOUNTS_FILE.exists():
        return []
    try:
        return json.loads(ACCOUNTS_FILE.read_text())
    except Exception:
        return []


def _save_accounts(accounts: list[dict]) -> None:
    ACCOUNTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    ACCOUNTS_FILE.write_text(json.dumps(accounts, indent=2))


# ── Request/Response Models ──────────────────────────────────────────


class AccountCreate(BaseModel):
    provider: ProviderType
    label: str = Field(..., description="Friendly name, e.g. 'College Results Mailbox'")

    # --- IMAP fields ---
    imap_host: str = ""
    imap_port: int = 993
    imap_use_ssl: bool = True
    imap_username: str = ""
    imap_password: str = ""
    imap_mailbox: str = "INBOX"
    # OAuth2 token for XOAUTH2 (Google Workspace IMAP / Outlook IMAP)
    imap_oauth2_token: str = ""

    # --- MS Graph / Office 365 fields ---
    msgraph_tenant_id: str = ""
    msgraph_client_id: str = ""
    msgraph_client_secret: str = ""
    msgraph_user_email: str = ""

    # --- Webhook fields (no secrets needed here; secret is in .env) ---
    webhook_provider: str = ""   # mailgun | sendgrid | postmark | custom


class AccountResponse(BaseModel):
    account_id: str
    provider: ProviderType
    label: str
    status: str   # connected | error | unchecked
    detail: str = ""
    # Masked/safe fields for display
    display_info: str = ""
    # Gmail OAuth profile (only set for gmail_oauth)
    profile_name: str = ""
    profile_picture: str = ""


# ── Endpoints ────────────────────────────────────────────────────────


def _get_gmail_oauth_account() -> dict | None:
    """
    Return a synthetic account entry if token.json exists and is valid.
    This ensures the Gmail OAuth account always appears in the list even
    if the user never added it via the Add Account modal.
    """
    from pathlib import Path as _P
    token_file = _P(__file__).resolve().parent.parent.parent.parent / "config" / "secrets" / "token.json"
    if not token_file.exists():
        return None
    try:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request as GRequest
        creds = Credentials.from_authorized_user_file(str(token_file))
        if creds.expired and creds.refresh_token:
            try:
                creds.refresh(GRequest())
            except Exception:
                pass
        status = "connected" if creds.valid else "error"
        detail = "OAuth2 token valid" if creds.valid else "Token expired — please reconnect"

        # Fetch user profile
        email, name, picture = "", "", ""
        if creds.valid:
            try:
                from googleapiclient.discovery import build
                svc = build("oauth2", "v2", credentials=creds)
                info = svc.userinfo().get().execute()
                email   = info.get("email", "")
                name    = info.get("name", "")
                picture = info.get("picture", "")
            except Exception:
                pass

        return {
            "account_id": "gmail_oauth",
            "provider": "gmail",
            "label": email or "Gmail (OAuth2)",
            "status": status,
            "detail": detail,
            "display_info": email,
            "profile_name": name,
            "profile_picture": picture,
        }
    except Exception:
        return None


@router.get("", response_model=list[AccountResponse])
async def list_accounts() -> list[AccountResponse]:
    """Return all configured email accounts (secrets masked)."""
    accounts = _load_accounts()

    # Always surface the Gmail OAuth account derived from token.json
    # (even if not manually added via the modal)
    existing_ids = {a["account_id"] for a in accounts}
    oauth_account = _get_gmail_oauth_account()
    if oauth_account and "gmail_oauth" not in existing_ids:
        accounts = [oauth_account] + accounts
    elif oauth_account:
        # Update status/display of the existing gmail_oauth entry
        for a in accounts:
            if a["account_id"] == "gmail_oauth":
                a.update({
                    "status": oauth_account["status"],
                    "detail": oauth_account["detail"],
                    "display_info": oauth_account["display_info"],
                    "label": oauth_account["label"],
                    "profile_name": oauth_account.get("profile_name", ""),
                    "profile_picture": oauth_account.get("profile_picture", ""),
                })

    return [_to_response(a) for a in accounts]


@router.post("", response_model=AccountResponse)
async def add_account(req: AccountCreate) -> AccountResponse:
    """Add or update an email account configuration."""
    accounts = _load_accounts()

    record: dict[str, Any] = {
        "account_id": str(uuid.uuid4()),
        "provider": req.provider,
        "label": req.label,
        "status": "unchecked",
        "detail": "",
    }

    if req.provider == "gmail":
        record["display_info"] = "OAuth2 — use Connect Gmail below"

    elif req.provider == "imap":
        if not req.imap_host or not req.imap_username:
            raise HTTPException(400, "imap_host and imap_username are required")
        record.update({
            "imap_host": req.imap_host,
            "imap_port": req.imap_port,
            "imap_use_ssl": req.imap_use_ssl,
            "imap_username": req.imap_username,
            "imap_password": req.imap_password,
            "imap_mailbox": req.imap_mailbox,
            "imap_oauth2_token": req.imap_oauth2_token,
            "display_info": f"{req.imap_username} @ {req.imap_host}:{req.imap_port}",
        })

    elif req.provider == "msgraph":
        if not req.msgraph_tenant_id or not req.msgraph_client_id or not req.msgraph_user_email:
            raise HTTPException(400, "tenant_id, client_id, and user_email are required")
        record.update({
            "msgraph_tenant_id": req.msgraph_tenant_id,
            "msgraph_client_id": req.msgraph_client_id,
            "msgraph_client_secret": req.msgraph_client_secret,
            "msgraph_user_email": req.msgraph_user_email,
            "display_info": req.msgraph_user_email,
        })

    elif req.provider == "webhook":
        relay = req.webhook_provider or "custom"
        record.update({
            "webhook_provider": relay,
            "display_info": f"Inbound relay: {relay}",
        })

    accounts.append(record)
    _save_accounts(accounts)
    logger.info("account_added", account_id=record["account_id"], provider=req.provider)
    return _to_response(record)


@router.delete("/{account_id}")
async def delete_account(account_id: str) -> dict:
    """Remove an email account."""
    accounts = _load_accounts()
    before = len(accounts)
    accounts = [a for a in accounts if a["account_id"] != account_id]
    if len(accounts) == before:
        raise HTTPException(404, "Account not found")
    _save_accounts(accounts)
    logger.info("account_deleted", account_id=account_id)
    return {"deleted": account_id}


@router.post("/{account_id}/test", response_model=AccountResponse)
async def test_account(account_id: str) -> AccountResponse:
    """Live-test the connection for an email account."""
    accounts = _load_accounts()
    record = next((a for a in accounts if a["account_id"] == account_id), None)
    if not record:
        raise HTTPException(404, "Account not found")

    provider = record["provider"]
    status = "error"
    detail = ""

    try:
        if provider == "gmail":
            from pathlib import Path as _P
            token_file = _P(__file__).resolve().parent.parent.parent.parent / "config" / "secrets" / "token.json"
            if token_file.exists():
                from google.oauth2.credentials import Credentials
                from google.auth.transport.requests import Request as GRequest
                creds = Credentials.from_authorized_user_file(str(token_file))
                if creds.expired and creds.refresh_token:
                    creds.refresh(GRequest())
                status = "connected" if creds.valid else "error"
                detail = "Token valid" if creds.valid else "Token expired / invalid"
                if creds.valid:
                    try:
                        from googleapiclient.discovery import build
                        svc = build("oauth2", "v2", credentials=creds)
                        info = svc.userinfo().get().execute()
                        record["profile_name"]    = info.get("name", "")
                        record["profile_picture"] = info.get("picture", "")
                        record["display_info"]    = info.get("email", record.get("display_info", ""))
                        record["label"]           = info.get("email", record.get("label", "Gmail"))
                    except Exception:
                        pass
            else:
                status = "error"
                detail = "Not authenticated — click Connect Gmail"

        elif provider == "imap":
            import imapclient
            import asyncio

            def _try_imap():
                ssl_ctx = ssl.create_default_context() if record.get("imap_use_ssl") else None
                with imapclient.IMAPClient(
                    record["imap_host"],
                    port=record["imap_port"],
                    ssl=record.get("imap_use_ssl", True),
                    ssl_context=ssl_ctx,
                    timeout=10,
                ) as client:
                    if record.get("imap_oauth2_token"):
                        auth_str = (
                            f"user={record['imap_username']}\x01"
                            f"auth=Bearer {record['imap_oauth2_token']}\x01\x01"
                        )
                        client.oauth2_login(record["imap_username"], auth_str)
                    else:
                        client.login(record["imap_username"], record.get("imap_password", ""))
                    capabilities = client.capabilities()
                return f"Login OK — caps: {', '.join(str(c) for c in capabilities[:5])}"

            loop = asyncio.get_event_loop()
            detail = await loop.run_in_executor(None, _try_imap)
            status = "connected"

        elif provider == "msgraph":
            token_url = (
                f"https://login.microsoftonline.com/"
                f"{record['msgraph_tenant_id']}/oauth2/v2.0/token"
            )
            payload = {
                "grant_type": "client_credentials",
                "client_id": record["msgraph_client_id"],
                "client_secret": record["msgraph_client_secret"],
                "scope": "https://graph.microsoft.com/.default",
            }
            async with aiohttp.ClientSession() as session:
                async with session.post(token_url, data=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    data = await resp.json()
            if "access_token" in data:
                # Verify we can list one message
                token = data["access_token"]
                url = (
                    f"https://graph.microsoft.com/v1.0/users/"
                    f"{record['msgraph_user_email']}/mailFolders/Inbox/messages?$top=1&$select=id"
                )
                async with aiohttp.ClientSession(headers={"Authorization": f"Bearer {token}"}) as s:
                    async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                        if r.status == 200:
                            status = "connected"
                            detail = f"Token OK — mailbox accessible ({record['msgraph_user_email']})"
                        else:
                            status = "error"
                            detail = f"Token OK but mailbox access failed: HTTP {r.status}"
            else:
                status = "error"
                detail = data.get("error_description", "Token request failed")

        elif provider == "webhook":
            status = "connected"
            detail = f"Webhook relay ready — POST to /webhooks/email ({record.get('webhook_provider','custom')})"

    except Exception as exc:
        status = "error"
        detail = str(exc)
        logger.warning("account_test_failed", account_id=account_id, error=str(exc))

    # Persist updated status
    for a in accounts:
        if a["account_id"] == account_id:
            a["status"] = status
            a["detail"] = detail
    _save_accounts(accounts)

    record["status"] = status
    record["detail"] = detail
    return _to_response(record)


# ── Helpers ──────────────────────────────────────────────────────────


def _to_response(a: dict) -> AccountResponse:
    return AccountResponse(
        account_id=a["account_id"],
        provider=a["provider"],
        label=a["label"],
        status=a.get("status", "unchecked"),
        detail=a.get("detail", ""),
        display_info=a.get("display_info", ""),
        profile_name=a.get("profile_name", ""),
        profile_picture=a.get("profile_picture", ""),
    )
