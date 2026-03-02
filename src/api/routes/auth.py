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
CREDENTIALS_FILE = PROJECT_ROOT / "credentials.json"
TOKEN_FILE = PROJECT_ROOT / "token.json"

# Gmail scopes: read-only email + profile info
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "openid",
]


def _get_redirect_uri() -> str:
    """Return the OAuth redirect URI pointing back to our callback."""
    return "http://localhost:8000/api/v1/auth/callback"


def _load_credentials() -> Credentials | None:
    """Load stored OAuth credentials from token.json."""
    if not TOKEN_FILE.exists():
        return None
    try:
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)
        return creds
    except Exception as e:
        logger.error("token_load_failed", error=str(e))
        return None


def _save_credentials(creds: Credentials) -> None:
    """Save OAuth credentials to token.json."""
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
async def auth_login():
    """
    Start Gmail OAuth2 flow.

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

    authorization_url, state = flow.authorization_url(
        access_type="offline",     # Get refresh token
        include_granted_scopes="true",
        prompt="consent",          # Force consent to get refresh_token every time
    )

    logger.info("oauth_redirect", url=authorization_url)
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
        flow.fetch_token(code=code)
        creds = flow.credentials
        _save_credentials(creds)

        # Get user info to log
        user_info = _get_user_info(creds)
        logger.info(
            "oauth_success",
            email=user_info.get("email", "unknown"),
        )

        # Redirect back to frontend with success flag
        return RedirectResponse(url="/?auth=success")

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
