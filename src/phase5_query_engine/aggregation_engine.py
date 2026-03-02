"""
Aggregation Engine.

Deterministic computation of academic aggregates:
  - SGPA (Semester Grade Point Average)
  - CGPA (Cumulative Grade Point Average)
  - Pass rates
  - Percentiles
  - Batch statistics

All computations follow Indian university norms (10-point scale,
credit-weighted averages).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.common.config import get_settings
from src.common.observability import get_logger

logger = get_logger(__name__)


# Standard grade-to-grade-point mapping (10-point scale)
GRADE_POINTS: dict[str, float] = {
    "O": 10.0,   # Outstanding
    "S": 10.0,   # Superior
    "A+": 9.0,
    "A": 8.0,
    "B+": 7.0,
    "B": 6.0,
    "C+": 5.0,
    "C": 4.0,
    "D": 3.0,    # Minimum pass
    "E": 0.0,    # Fail
    "F": 0.0,    # Fail
    "FE": 0.0,   # Fail (Eligibility)
    "AB": 0.0,   # Absent
}


@dataclass
class SGPAResult:
    """Result of SGPA computation."""
    sgpa: float
    credits_earned: int
    credits_attempted: int
    subjects_passed: int
    subjects_failed: int
    grade_point_sum: float


@dataclass
class CGPAResult:
    """Result of CGPA computation."""
    cgpa: float
    total_credits_earned: int
    total_credits_attempted: int
    total_subjects_passed: int
    total_subjects_failed: int
    semesters_completed: int


@dataclass
class BatchStatistics:
    """Statistics for a batch/department."""
    total_students: int
    avg_cgpa: float
    median_cgpa: float
    min_cgpa: float
    max_cgpa: float
    std_cgpa: float
    pass_rate: float
    avg_backlogs: float
    percentiles: dict[int, float]  # {25: 6.5, 50: 7.2, 75: 8.1, 90: 8.8}


class AggregationEngine:
    """
    Compute deterministic academic aggregates.

    All computations use credit-weighted formulas:
      SGPA = Σ(grade_points × credits) / Σ(credits)
      CGPA = Σ(SGPA_i × semester_credits_i) / Σ(semester_credits_i)

    Deterministic: Same inputs ALWAYS produce same outputs.
    No LLM involvement in computation.
    """

    def __init__(self) -> None:
        self.settings = get_settings()
        self._db_pool = None

    def compute_sgpa(
        self,
        subject_results: list[dict[str, Any]],
    ) -> SGPAResult:
        """
        Compute SGPA from subject results.

        Args:
            subject_results: List of dicts with keys:
                - grade: str
                - credits: int
                - status: str (PASS/FAIL/ABSENT)

        Returns:
            SGPAResult with SGPA and credit summary
        """
        total_grade_points = 0.0
        total_credits = 0
        credits_earned = 0
        passed = 0
        failed = 0

        for subj in subject_results:
            grade = subj.get("grade", "F").upper()
            credits = int(subj.get("credits", 0))
            status = subj.get("status", "PASS").upper()

            grade_point = GRADE_POINTS.get(grade, 0.0)
            total_grade_points += grade_point * credits
            total_credits += credits

            if status == "PASS" and grade_point > 0:
                credits_earned += credits
                passed += 1
            else:
                failed += 1

        sgpa = (
            round(total_grade_points / total_credits, 2)
            if total_credits > 0
            else 0.0
        )

        return SGPAResult(
            sgpa=sgpa,
            credits_earned=credits_earned,
            credits_attempted=total_credits,
            subjects_passed=passed,
            subjects_failed=failed,
            grade_point_sum=total_grade_points,
        )

    def compute_cgpa(
        self,
        semester_results: list[SGPAResult],
    ) -> CGPAResult:
        """
        Compute CGPA from multiple semesters.

        Uses credit-weighted average of SGPAs.
        """
        total_weighted = 0.0
        total_credits = 0
        total_earned = 0
        total_attempted = 0
        total_passed = 0
        total_failed = 0

        for sem in semester_results:
            total_weighted += sem.sgpa * sem.credits_attempted
            total_credits += sem.credits_attempted
            total_earned += sem.credits_earned
            total_attempted += sem.credits_attempted
            total_passed += sem.subjects_passed
            total_failed += sem.subjects_failed

        cgpa = (
            round(total_weighted / total_credits, 2)
            if total_credits > 0
            else 0.0
        )

        return CGPAResult(
            cgpa=cgpa,
            total_credits_earned=total_earned,
            total_credits_attempted=total_attempted,
            total_subjects_passed=total_passed,
            total_subjects_failed=total_failed,
            semesters_completed=len(semester_results),
        )

    def compute_batch_statistics(
        self,
        student_cgpas: list[float],
        student_backlogs: list[int] | None = None,
    ) -> BatchStatistics:
        """Compute statistics for a batch of students."""
        import math

        n = len(student_cgpas)
        if n == 0:
            return BatchStatistics(
                total_students=0,
                avg_cgpa=0.0,
                median_cgpa=0.0,
                min_cgpa=0.0,
                max_cgpa=0.0,
                std_cgpa=0.0,
                pass_rate=0.0,
                avg_backlogs=0.0,
                percentiles={},
            )

        sorted_cgpas = sorted(student_cgpas)
        avg = sum(sorted_cgpas) / n
        median = self._percentile(sorted_cgpas, 50)
        variance = sum((x - avg) ** 2 for x in sorted_cgpas) / n
        std = math.sqrt(variance)

        # Pass rate: CGPA > 0 considered "active/passing"
        passing = sum(1 for c in sorted_cgpas if c > 0)
        pass_rate = round(100 * passing / n, 1)

        percentiles = {
            p: self._percentile(sorted_cgpas, p)
            for p in [10, 25, 50, 75, 90, 95]
        }

        avg_backlogs = 0.0
        if student_backlogs:
            avg_backlogs = round(
                sum(student_backlogs) / len(student_backlogs), 1
            )

        return BatchStatistics(
            total_students=n,
            avg_cgpa=round(avg, 2),
            median_cgpa=round(median, 2),
            min_cgpa=round(min(sorted_cgpas), 2),
            max_cgpa=round(max(sorted_cgpas), 2),
            std_cgpa=round(std, 2),
            pass_rate=pass_rate,
            avg_backlogs=avg_backlogs,
            percentiles={k: round(v, 2) for k, v in percentiles.items()},
        )

    async def recompute_student_aggregates(
        self,
        student_id: int,
    ) -> CGPAResult:
        """
        Recompute all aggregates for a student from raw results.

        This is the authoritative computation — called after new
        results are stored or corrections are made.
        """
        pool = await self._get_pool()
        if not pool:
            raise RuntimeError("Database not available")

        async with pool.acquire() as conn:
            # Fetch all results grouped by semester
            rows = await conn.fetch(
                """
                SELECT
                    sr.semester,
                    sr.grade,
                    sub.credits,
                    sr.status
                FROM student_results sr
                JOIN subjects sub ON sub.id = sr.subject_id
                WHERE sr.student_id = $1
                ORDER BY sr.semester
                """,
                student_id,
            )

        # Group by semester
        semesters: dict[int, list[dict]] = {}
        for row in rows:
            sem = row["semester"]
            semesters.setdefault(sem, []).append(dict(row))

        # Compute SGPA per semester
        semester_sgpas: list[SGPAResult] = []
        for sem_num in sorted(semesters.keys()):
            sgpa_result = self.compute_sgpa(semesters[sem_num])
            semester_sgpas.append(sgpa_result)

            # Update semester_aggregates table
            if pool:
                async with pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO semester_aggregates
                            (student_id, semester, sgpa, credits_earned,
                             credits_attempted, subjects_passed, subjects_failed)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (student_id, semester)
                        DO UPDATE SET
                            sgpa = EXCLUDED.sgpa,
                            credits_earned = EXCLUDED.credits_earned,
                            credits_attempted = EXCLUDED.credits_attempted,
                            subjects_passed = EXCLUDED.subjects_passed,
                            subjects_failed = EXCLUDED.subjects_failed
                        """,
                        student_id,
                        sem_num,
                        sgpa_result.sgpa,
                        sgpa_result.credits_earned,
                        sgpa_result.credits_attempted,
                        sgpa_result.subjects_passed,
                        sgpa_result.subjects_failed,
                    )

        # Compute CGPA
        cgpa_result = self.compute_cgpa(semester_sgpas)

        # Update student record
        if pool:
            total_backlogs = sum(s.subjects_failed for s in semester_sgpas)
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE students
                    SET current_cgpa = $2,
                        total_credits = $3,
                        active_backlogs = $4,
                        updated_at = NOW()
                    WHERE id = $1
                    """,
                    student_id,
                    cgpa_result.cgpa,
                    cgpa_result.total_credits_earned,
                    total_backlogs,
                )

        logger.info(
            "student_aggregates_recomputed",
            student_id=student_id,
            cgpa=cgpa_result.cgpa,
            semesters=cgpa_result.semesters_completed,
        )

        return cgpa_result

    @staticmethod
    def _percentile(sorted_data: list[float], p: int) -> float:
        """Compute p-th percentile of sorted data."""
        n = len(sorted_data)
        if n == 0:
            return 0.0
        k = (p / 100) * (n - 1)
        f = int(k)
        c = f + 1
        if c >= n:
            return sorted_data[-1]
        return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])

    async def _get_pool(self):
        """Get or create database pool."""
        if self._db_pool is None:
            try:
                import asyncpg
                self._db_pool = await asyncpg.create_pool(
                    dsn=get_settings().database.url,
                    min_size=2,
                    max_size=5,
                )
            except Exception as e:
                logger.warning("db_pool_init_failed", error=str(e))
                return None
        return self._db_pool
