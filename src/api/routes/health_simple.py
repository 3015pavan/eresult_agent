"""
Simple Health Endpoint
"""

from fastapi import APIRouter
from src.common.observability import get_logger

logger = get_logger(__name__)
router = APIRouter()

@router.get("/health")
async def health_check():
    """Simple health check endpoint"""
    try:
        from src.common.database import get_connection
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                return {
                    "status": "healthy",
                    "database": "connected",
                    "message": "AcadExtract is running perfectly"
                }
    except Exception as e:
        logger.error("health_check_failed", error=str(e))
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "message": f"Health check failed: {str(e)}"
        }
