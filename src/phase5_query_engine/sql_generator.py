"""Minimal structured SQL generator for common read-only teacher queries."""

from __future__ import annotations

from src.common.models import ParsedQuery, QueryIntent, ResultStatus


class SecurityError(ValueError):
    """Raised when unsafe SQL is attempted."""


def _sqlglot_validate(sql: str) -> None:
    """
    Parse with sqlglot and reject any non-SELECT statements.
    Falls back to keyword check if sqlglot is not installed.
    """
    try:
        import sqlglot
        statements = sqlglot.parse(sql)
        for stmt in statements:
            if stmt is None:
                continue
            # sqlglot uses class names like Select, Drop, Insert, etc.
            stmt_type = type(stmt).__name__
            if stmt_type != "Select":
                raise SecurityError(f"Only SELECT allowed; got {stmt_type}")
    except ImportError:
        # Fallback: keyword blocklist
        normalized = (sql or "").upper()
        for kw in ("DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE"):
            if kw in normalized:
                raise SecurityError(f"Forbidden keyword: {kw}")


class SQLGenerator:
    """Generate parameterized, read-only SQL snippets for common intents.

    Uses sqlglot for AST-level validation to detect non-SELECT statements
    regardless of quoting or comment injection tricks.
    """

    FORBIDDEN = ("DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE")

    def _validate_sql(self, sql: str) -> None:
        _sqlglot_validate(sql)

    def generate(self, parsed: ParsedQuery) -> tuple[str, list]:
        intent = parsed.intent
        entities = parsed.entities or {}
        filters = parsed.filters or {}

        if intent == QueryIntent.STUDENT_LOOKUP:
            sql = (
                "SELECT s.usn, s.name, s.cgpa, s.total_backlogs "
                "FROM students s WHERE UPPER(s.usn) = UPPER($1)"
            )
            params = [entities.get("usn") or parsed.student_usn]
        elif intent == QueryIntent.TOP_N:
            limit = int(entities.get("limit", 10) or 10)
            metric = str(entities.get("metric", "cgpa") or "cgpa").lower()
            metric_col = "cgpa" if metric not in {"sgpa", "total_backlogs"} else metric
            sql = (
                f"SELECT usn, name, {metric_col} FROM students "
                f"WHERE {metric_col} IS NOT NULL ORDER BY {metric_col} DESC LIMIT $1"
            )
            params = [limit]
        elif intent == QueryIntent.COUNT:
            sql = "SELECT COUNT(*) FROM student_results WHERE 1=1"
            params = []
            status = filters.get("status") or parsed.status_filter
            if status:
                sql += " AND status = $1"
                params.append(status.value if isinstance(status, ResultStatus) else status)
        elif intent in {QueryIntent.AGGREGATION, QueryIntent.STUDENT_GPA, QueryIntent.STUDENT_CGPA}:
            sql = "SELECT COUNT(*) AS total_students, AVG(cgpa) AS avg_cgpa FROM students"
            params = []
        elif intent in {QueryIntent.BACKLOGS, QueryIntent.BACKLOG_CHECK}:
            sql = (
                "SELECT usn, name, total_backlogs FROM students "
                "WHERE total_backlogs > 0 ORDER BY total_backlogs DESC"
            )
            params = []
        else:
            sql = "SELECT 1"
            params = []

        self._validate_sql(sql)
        return sql, params