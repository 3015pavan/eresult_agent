"""
Direct Gmail Sync — no Redis, no S3 required.

Fetches real emails from the connected Gmail/Google Workspace account
using the stored config/secrets/token.json, processes them through the
pipeline, and stores results locally in data/emails_cache.json.

Endpoints:
  POST /api/v1/sync          — fetch recent emails
  GET  /api/v1/sync/emails   — list fetched emails
  GET  /api/v1/sync/status   — last sync info
"""

from __future__ import annotations

import json
import base64
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.common.observability import get_logger

logger = get_logger(__name__)
router = APIRouter()

PROJECT_ROOT   = Path(__file__).resolve().parent.parent.parent.parent
TOKEN_FILE     = PROJECT_ROOT / "config" / "secrets" / "token.json"
EMAILS_CACHE   = PROJECT_ROOT / "data" / "emails_cache.json"
SYNC_STATE     = PROJECT_ROOT / "data" / "state" / "sync_state.json"

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "openid",
]


# ── helpers ─────────────────────────────────────────────────────────

def _load_creds():
    """Load and auto-refresh OAuth credentials. Returns None if not connected."""
    if not TOKEN_FILE.exists():
        return None
    try:
        import json as _json
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        data = _json.loads(TOKEN_FILE.read_text())
        if not data.get("refresh_token"):
            logger.warning("sync_creds_missing_refresh_token")
            return None
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            TOKEN_FILE.write_text(creds.to_json())
        return creds if creds.valid else None
    except Exception as e:
        logger.error("creds_load_failed", error=str(e))
        return None


def _decode_body(part: dict) -> str:
    """Decode a Gmail message part body from base64url."""
    data = part.get("body", {}).get("data", "")
    if not data:
        return ""
    try:
        return base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")
    except Exception:
        return ""


def _extract_text(payload: dict) -> str:
    """Recursively extract plain text from a Gmail message payload."""
    mime = payload.get("mimeType", "")
    if mime == "text/plain":
        return _decode_body(payload)
    if mime.startswith("multipart/"):
        parts = payload.get("parts", [])
        # prefer plain text over html
        texts = [_extract_text(p) for p in parts]
        return "\n".join(t for t in texts if t).strip()
    return ""


def _parse_message(raw: dict) -> dict:
    """Convert Gmail API message object to a clean dict."""
    headers = {h["name"].lower(): h["value"]
               for h in raw.get("payload", {}).get("headers", [])}

    # attachments
    attachments = []
    def _walk(part):
        filename = part.get("filename", "")
        mime = part.get("mimeType", "")
        if filename:
            attachments.append({
                "filename": filename,
                "mimeType": mime,
                "size": part.get("body", {}).get("size", 0),
                "attachmentId": part.get("body", {}).get("attachmentId", ""),
            })
        for sub in part.get("parts", []):
            _walk(sub)
    _walk(raw.get("payload", {}))

    body = _extract_text(raw.get("payload", {}))

    # Thread reconstruction headers
    message_id_header = headers.get("message-id", "").strip("<>")
    in_reply_to       = headers.get("in-reply-to", "").strip("<>")
    references_raw    = headers.get("references", "")
    # References is a whitespace-delimited list of Message-IDs
    references = [r.strip("<>") for r in references_raw.split() if r.strip("<>")]
    # Thread depth = number of ancestors referenced
    thread_depth = len(references) if references else (1 if in_reply_to else 0)

    return {
        "id":               raw["id"],
        "threadId":         raw.get("threadId", ""),
        "message_id":       message_id_header,
        "in_reply_to":      in_reply_to,
        "references":       references,
        "thread_depth":     thread_depth,
        "subject":          headers.get("subject", "(no subject)"),
        "from":             headers.get("from", ""),
        "to":               headers.get("to", ""),
        "cc":               headers.get("cc", ""),
        "date":             headers.get("date", ""),
        "snippet":          raw.get("snippet", ""),
        "body":             body[:2000],          # keep first 2k chars
        "attachments":      attachments,
        "labels":           raw.get("labelIds", []),
    }


def _load_cache() -> list[dict]:
    if EMAILS_CACHE.exists():
        try:
            return json.loads(EMAILS_CACHE.read_text())
        except Exception:
            return []
    return []


def _save_cache(emails: list[dict]):
    EMAILS_CACHE.parent.mkdir(parents=True, exist_ok=True)
    EMAILS_CACHE.write_text(json.dumps(emails, indent=2, default=str))


def _load_sync_state() -> dict:
    if SYNC_STATE.exists():
        try:
            return json.loads(SYNC_STATE.read_text())
        except Exception:
            return {}
    return {}


def _save_sync_state(state: dict):
    SYNC_STATE.parent.mkdir(parents=True, exist_ok=True)
    SYNC_STATE.write_text(json.dumps(state, indent=2, default=str))


# ── request / response models ────────────────────────────────────────

class SyncRequest(BaseModel):
    max_results: int = 200
    query: str = ""           # Gmail search query override
    since_days: int = 90       # look back 90 days by default
    all_folders: bool = True   # search inbox + spam + all mail


# ── endpoints ────────────────────────────────────────────────────────

@router.post("/sync")
async def trigger_sync(req: SyncRequest = SyncRequest()) -> dict:
    """
    Fetch recent emails from the connected Gmail account.
    Stores results in emails_cache.json.
    """
    creds = _load_creds()
    if not creds:
        raise HTTPException(
            status_code=401,
            detail="Gmail not connected. Go to Email Accounts → Add Account → Continue with Google.",
        )

    try:
        from googleapiclient.discovery import build
        service = build("gmail", "v1", credentials=creds)

        # Build search query — result keywords across ALL folders
        RESULT_KEYWORDS = (
            "result OR marks OR grade OR sgpa OR cgpa OR marksheet OR "
            "\"semester result\" OR \"examination result\" OR backlog OR "
            "\"grade card\" OR transcript OR \"internal marks\""
        )
        q_parts = []
        if req.since_days:
            q_parts.append(f"newer_than:{req.since_days}d")
        if req.query:
            # caller-supplied override
            q_parts.append(req.query)
        else:
            # Search everywhere (inbox + spam + all mail) for result-related emails
            if req.all_folders:
                q_parts.append(f"in:anywhere ({RESULT_KEYWORDS})")
            else:
                q_parts.append(RESULT_KEYWORDS)
        q = " ".join(q_parts)

        logger.info("gmail_sync_start", query=q, max_results=req.max_results)

        # List message IDs
        list_resp = service.users().messages().list(
            userId="me",
            q=q,
            maxResults=req.max_results,
        ).execute()

        messages_meta = list_resp.get("messages", [])

        if not messages_meta:
            _save_sync_state({
                "last_sync": datetime.now(timezone.utc).isoformat(),
                "fetched": 0,
                "query": q,
            })
            return {"fetched": 0, "message": "No emails found matching the query."}

        # Fetch full messages
        emails = []
        for meta in messages_meta:
            try:
                raw = service.users().messages().get(
                    userId="me",
                    id=meta["id"],
                    format="full",
                ).execute()
                emails.append(_parse_message(raw))
            except Exception as e:
                logger.warning("message_fetch_failed", id=meta["id"], error=str(e))

        # Merge with existing cache (avoid duplicates by id)
        existing = {e["id"]: e for e in _load_cache()}
        for e in emails:
            existing[e["id"]] = e
        merged = list(existing.values())
        _save_cache(merged)

        state = {
            "last_sync": datetime.now(timezone.utc).isoformat(),
            "fetched": len(emails),
            "total_cached": len(merged),
            "query": q,
        }
        _save_sync_state(state)

        logger.info("gmail_sync_done", fetched=len(emails), total=len(merged))
        return {
            "fetched": len(emails),
            "total_cached": len(merged),
            "last_sync": state["last_sync"],
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("sync_failed", error=str(e))
        raise HTTPException(status_code=500, detail=f"Sync failed: {e}")


@router.get("/sync/emails")
async def list_emails(
    limit: int = 50,
    offset: int = 0,
    q: str = "",
    classification: str = "",   # e.g. "result_email" to show only result emails
) -> dict:
    """Return cached emails, optionally filtered by text or classification."""
    emails = _load_cache()

    # ── Inject classification from DB ───────────────────────────────────────
    # Build a map of message_id → classification from email_metadata
    try:
        from src.common import database as db
        db.init_db()
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT message_id, classification, status FROM email_metadata"
                )
                _clf_map = {row[0]: (row[1], row[2]) for row in cur.fetchall()}
    except Exception:
        _clf_map = {}

    for e in emails:
        mid = e.get("id", "")
        if mid in _clf_map:
            e["classification"] = _clf_map[mid][0]
            e["pipeline_status"] = _clf_map[mid][1]
        else:
            e.setdefault("classification", "unknown")
            e.setdefault("pipeline_status", "")

    # ── Classification filter ────────────────────────────────────────────────
    if classification:
        emails = [e for e in emails if e.get("classification") == classification]

    # ── Text search filter ───────────────────────────────────────────────────
    if q:
        ql = q.lower()
        emails = [
            e for e in emails
            if ql in e.get("subject", "").lower()
            or ql in e.get("from", "").lower()
            or ql in e.get("snippet", "").lower()
            or ql in e.get("body", "").lower()
        ]

    # newest first
    emails = list(reversed(emails))
    total = len(emails)
    page  = emails[offset: offset + limit]

    return {"total": total, "emails": page}


@router.get("/sync/status")
async def sync_status() -> dict:
    """Return last sync state + Gmail connection status."""
    state = _load_sync_state()
    creds = _load_creds()
    connected = creds is not None

    account_email = ""
    if connected:
        try:
            from googleapiclient.discovery import build
            svc = build("oauth2", "v2", credentials=creds)
            info = svc.userinfo().get().execute()
            account_email = info.get("email", "")
        except Exception:
            pass

    return {
        "connected": connected,
        "account": account_email,
        "last_sync": state.get("last_sync"),
        "total_cached": state.get("total_cached", len(_load_cache())),
        "fetched_last_run": state.get("fetched", 0),
    }


# ── Thread Reconstruction ────────────────────────────────────────────


def _build_thread_tree(emails: list[dict]) -> list[dict]:
    """
    Group emails into conversation threads using Message-ID/In-Reply-To/References headers.

    Returns a list of root messages, each with a `replies` key containing
    their direct children recursively.
    """
    # Index all emails by their Message-ID header for O(1) parent lookup
    by_msg_id: dict[str, dict] = {}
    for e in emails:
        mid = e.get("message_id") or e.get("id", "")
        if mid:
            by_msg_id[mid] = {**e, "replies": []}

    roots: list[dict] = []

    for e in emails:
        mid = e.get("message_id") or e.get("id", "")
        if not mid or mid not in by_msg_id:
            continue
        node = by_msg_id[mid]

        # Try In-Reply-To first, then last entry of References
        parent_id = e.get("in_reply_to", "")
        if not parent_id:
            refs = e.get("references", [])
            parent_id = refs[-1] if refs else ""

        if parent_id and parent_id in by_msg_id:
            by_msg_id[parent_id]["replies"].append(node)
        else:
            # No recognised parent → this is a root message
            roots.append(node)

    # Sort roots and replies by date descending (newest first)
    def _sort_key(n: dict) -> str:
        return n.get("date", "")

    roots.sort(key=_sort_key, reverse=True)
    for node in by_msg_id.values():
        node["replies"].sort(key=_sort_key)

    return roots


@router.get("/sync/threads")
async def get_threads(limit: int = 20, q: str = "") -> dict:
    """
    Return cached emails grouped into conversation threads.

    Each thread root contains a `replies` list of direct children
    (recursively). Useful for reviewing related result emails together.
    """
    emails = _load_cache()

    # Optional text filter before threading
    if q:
        ql = q.lower()
        emails = [
            e for e in emails
            if ql in e.get("subject", "").lower()
            or ql in e.get("from", "").lower()
            or ql in e.get("body", "").lower()
        ]

    threads = _build_thread_tree(emails)
    return {
        "total_threads": len(threads),
        "total_emails": len(emails),
        "threads": threads[:limit],
    }


# ── IMAP Sync ────────────────────────────────────────────────────────


class ImapSyncRequest(BaseModel):
    account_id: str
    institution_id: str = ""


@router.post("/sync/imap")
async def trigger_imap_sync(req: ImapSyncRequest) -> dict:
    """
    Trigger IMAP inbox sync for a configured IMAP account.
    Dispatches a Celery `sync_imap_inbox` task for the given account_id.
    Falls back to synchronous fetch if Celery is unavailable.
    """
    institution_id = req.institution_id or None
    try:
        from src.tasks.ingestion import sync_imap_inbox
        task = sync_imap_inbox.apply_async(
            kwargs={"account_id": req.account_id, "institution_id": institution_id},
            queue="email_ingestion",
        )
        return {
            "status": "enqueued",
            "task_id": task.id,
            "account_id": req.account_id,
        }
    except Exception:
        # Celery unavailable — run synchronously
        try:
            from src.tasks.ingestion import sync_imap_inbox
            result = sync_imap_inbox(req.account_id, institution_id)
            return {**result, "mode": "sync"}
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"IMAP sync failed: {exc}")
