"""
Security layer — ClamAV virus scanning.

Wraps clamd (ClamAV daemon) for attachment scanning.
Falls back gracefully when ClamAV is not available.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

_CLAMD_HOST = os.getenv("CLAMD_HOST", "localhost")
_CLAMD_PORT = int(os.getenv("CLAMD_PORT", "3310"))


class ScanResult:
    __slots__ = ("clean", "threat", "error")

    def __init__(self, clean: bool, threat: Optional[str] = None, error: Optional[str] = None):
        self.clean = clean
        self.threat = threat
        self.error = error

    def __repr__(self) -> str:
        if self.error:
            return f"ScanResult(error={self.error!r})"
        return f"ScanResult(clean={self.clean}, threat={self.threat!r})"


def scan_bytes(data: bytes) -> ScanResult:
    """
    Scan raw bytes with ClamAV via network socket.
    Returns ScanResult(clean=True) when ClamAV is unavailable (fail-open policy).
    """
    try:
        import clamd  # type: ignore
        cd = clamd.ClamdNetworkSocket(host=_CLAMD_HOST, port=_CLAMD_PORT, timeout=10)
        result = cd.instream(data)
        status, signature = next(iter(result.values()))
        if status == "OK":
            return ScanResult(clean=True)
        return ScanResult(clean=False, threat=signature)
    except ImportError:
        logger.debug("clamd package not installed — skipping AV scan")
        return ScanResult(clean=True, error="clamd_not_installed")
    except ConnectionRefusedError:
        logger.warning("ClamAV daemon not reachable at %s:%d — skipping scan", _CLAMD_HOST, _CLAMD_PORT)
        return ScanResult(clean=True, error="clamd_unavailable")
    except Exception as exc:
        logger.warning("ClamAV scan failed: %s", exc)
        return ScanResult(clean=True, error=str(exc))


def scan_file(path: str) -> ScanResult:
    """Scan a file on disk with ClamAV."""
    try:
        with open(path, "rb") as fh:
            return scan_bytes(fh.read())
    except OSError as exc:
        return ScanResult(clean=True, error=f"file_read_error: {exc}")


def is_safe(data: bytes) -> bool:
    """Convenience wrapper — True means safe to process."""
    return scan_bytes(data).clean
