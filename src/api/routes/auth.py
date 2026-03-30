"""
Gmail OAuth2 Authentication Routes.

Provides:
  - GET  /api/v1/auth/status   — Check if Gmail is connected
  - GET  /api/v1/auth/login    — Redirect to Google OAuth consent screen
  - GET  /api/v1/auth/callback — Handle OAuth callback, store token
  - POST /api/v1/auth/logout   — Revoke token and disconnect
"""

from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import RedirectResponse
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

from src.common.observability import get_logger

logger = get_logger(__name__)
router = APIRouter()

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
CREDENTIALS_FILE   = PROJECT_ROOT / "config" / "secrets" / "credentials.json"
TOKEN_FILE         = PROJECT_ROOT / "config" / "secrets" / "token.json"
CODE_VERIFIER_FILE = PROJECT_ROOT / "data" / "state" / "oauth_verifier.txt"  # temp PKCE store

# Gmail scopes: read-only email + profile info
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "openid",
]


def _get_redirect_uri() -> str:
    """Return the OAuth redirect URI pointing back to our callback."""
    import os
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(PROJECT_ROOT / ".env", override=False)
    except Exception:
        pass
    explicit = (os.getenv("GMAIL_REDIRECT_URI") or "").strip()
    if explicit:
        return explicit
    port = os.getenv("APP_PORT", "8002")
    return f"http://localhost:{port}/api/v1/auth/callback"


def _load_credentials() -> Credentials | None:
    """Load stored OAuth credentials from token.json."""
    if not TOKEN_FILE.exists():
        return None
    try:
        data = json.loads(TOKEN_FILE.read_text())
        if not data.get("refresh_token"):
            # Token has no refresh_token — treat as disconnected so we force
            # a fresh consent and get a proper offline token next login.
            logger.warning("token_missing_refresh_token")
            return None
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)
        return creds
    except Exception as e:
        logger.error("token_load_failed", error=str(e))
        return None


def _save_credentials(creds: Credentials) -> None:
    """Save OAuth credentials to token.json."""
    TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_FILE.write_text(creds.to_json())
    logger.info("token_saved", path=str(TOKEN_FILE))


def _get_user_info(creds: Credentials) -> dict:
    """Fetch basic user profile from Google."""
    try:
        from googleapiclient.discovery import build
        service = build("oauth2", "v2", credentials=creds)
        user_info = service.userinfo().get().execute()
        return {
            "email": user_info.get("email", ""),
            "name": user_info.get("name", ""),
            "picture": user_info.get("picture", ""),
        }
    except Exception as e:
        logger.error("user_info_fetch_failed", error=str(e))
        return {}


# ── Endpoints ───────────────────────────────────────────────────────


@router.get("/auth/status")
async def auth_status() -> dict:
    """
    Check Gmail connection status.

    Returns:
      - connected: bool
      - email: str (if connected)
      - name: str (if connected)
      - valid: bool (token still valid)
    """
    creds = _load_credentials()
    if not creds:
        return {"connected": False}

    # Try to refresh if expired
    if creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            _save_credentials(creds)
        except Exception as e:
            logger.warning("token_refresh_failed", error=str(e))
            return {"connected": False, "error": "Token expired, please re-login"}

    if not creds.valid:
        return {"connected": False, "error": "Token invalid"}

    # Get user info
    user_info = _get_user_info(creds)

    return {
        "connected": True,
        "valid": True,
        "email": user_info.get("email", "unknown"),
        "name": user_info.get("name", ""),
        "picture": user_info.get("picture", ""),
    }


@router.get("/auth/login")
async def auth_login(hint: str = None):
    """
    Start Gmail OAuth2 flow.

    Accepts an optional `hint` query param (the user's email address) so
    Google pre-selects that account on the consent screen.
    Redirects the user to Google's consent screen.
    After consent, Google redirects back to /api/v1/auth/callback.
    """
    if not CREDENTIALS_FILE.exists():
        raise HTTPException(
            status_code=500,
            detail="credentials.json not found. Download it from Google Cloud Console.",
        )

    flow = Flow.from_client_secrets_file(
        str(CREDENTIALS_FILE),
        scopes=SCOPES,
        redirect_uri=_get_redirect_uri(),
    )

    # Only force consent screen when we don't already have a valid token.
    # login_hint pre-selects (or auto-selects) the right Google account.
    existing = _load_credentials()
    needs_consent = not existing or not existing.valid

    auth_kwargs: dict = {
        "access_type": "offline",
        "include_granted_scopes": "true",
    }
    if hint:
        auth_kwargs["login_hint"] = hint
    if needs_consent:
        auth_kwargs["prompt"] = "consent"  # ensures we get a refresh_token
    else:
        auth_kwargs["prompt"] = "select_account"  # let user pick if multi-account

    authorization_url, state = flow.authorization_url(**auth_kwargs)

    # If the library generated a PKCE code_verifier, persist it so the
    # callback (which creates a new Flow instance) can use it.
    verifier = getattr(flow, "code_verifier", None)
    if verifier:
        CODE_VERIFIER_FILE.parent.mkdir(parents=True, exist_ok=True)
        CODE_VERIFIER_FILE.write_text(verifier)
    elif CODE_VERIFIER_FILE.exists():
        CODE_VERIFIER_FILE.unlink()  # clean up stale verifier from a previous attempt

    logger.info("oauth_redirect", hint=hint, url=authorization_url)
    return RedirectResponse(url=authorization_url)


@router.get("/auth/callback")
async def auth_callback(code: str = None, error: str = None, state: str = None):
    """
    Handle OAuth2 callback from Google.

    Exchanges the authorization code for tokens and stores them.
    Redirects back to the frontend dashboard.
    """
    if error:
        logger.error("oauth_callback_error", error=error)
        return RedirectResponse(url="/?auth=error&message=" + error)

    if not code:
        raise HTTPException(status_code=400, detail="Missing authorization code")

    try:
        flow = Flow.from_client_secrets_file(
            str(CREDENTIALS_FILE),
            scopes=SCOPES,
            redirect_uri=_get_redirect_uri(),
        )

        # Restore PKCE code_verifier if the login step saved one
        if CODE_VERIFIER_FILE.exists():
            flow.code_verifier = CODE_VERIFIER_FILE.read_text().strip()
            CODE_VERIFIER_FILE.unlink()

        flow.fetch_token(code=code)
        creds = flow.credentials
        _save_credentials(creds)

        # Get user info to log
        user_info = _get_user_info(creds)
        logger.info(
            "oauth_success",
            email=user_info.get("email", "unknown"),
        )

        # Redirect back to frontend — ?sync=1 tells the UI to auto-sync emails
        return RedirectResponse(url="/?sync=1")

    except Exception as e:
        logger.error("oauth_token_exchange_failed", error=str(e))
        import urllib.parse
        msg = urllib.parse.quote(str(e)[:200])
        return RedirectResponse(url=f"/?auth=error&message={msg}")


@router.post("/auth/logout")
async def auth_logout() -> dict:
    """
    Disconnect Gmail account.

    Revokes the token and deletes token.json.
    """
    creds = _load_credentials()

    if creds and creds.token:
        try:
            import requests
            requests.post(
                "https://oauth2.googleapis.com/revoke",
                params={"token": creds.token},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            logger.info("token_revoked")
        except Exception as e:
            logger.warning("token_revoke_failed", error=str(e))

    # Delete token file
    if TOKEN_FILE.exists():
        TOKEN_FILE.unlink()
        logger.info("token_file_deleted")

    return {"disconnected": True}
