"""Deterministic SGPA/CGPA/statistics helpers for academic result data."""

from __future__ import annotations

from dataclasses import dataclass
from math import sqrt


GRADE_POINTS = {
    "O": 10.0,
    "A+": 9.0,
    "A": 8.0,
    "B+": 7.0,
    "B": 6.0,
    "C": 5.0,
    "P": 4.0,
    "D": 4.0,
    "F": 0.0,
}


@dataclass
class SGPAResult:
    sgpa: float
    credits_earned: int
    credits_attempted: int
    subjects_passed: int
    subjects_failed: int
    grade_point_sum: float


@dataclass
class CGPAResult:
    cgpa: float
    semesters_completed: int
    total_subjects_passed: int
    total_subjects_failed: int
    total_credits_earned: int


@dataclass
class BatchStatistics:
    total_students: int
    avg_cgpa: float
    min_cgpa: float
    max_cgpa: float
    std_cgpa: float
    percentiles: dict[int, float]


class AggregationEngine:
    """Pure-Python academic metric computations."""

    def compute_sgpa(self, subjects: list[dict]) -> SGPAResult:
        credits_attempted = 0
        credits_earned = 0
        subjects_passed = 0
        subjects_failed = 0
        grade_point_sum = 0.0

        for subject in subjects:
            credits = int(subject.get("credits", 0) or 0)
            status = str(subject.get("status", "PASS") or "PASS").upper()
            grade = str(subject.get("grade", "F") or "F").upper()
            points = GRADE_POINTS.get(grade, 0.0)

            credits_attempted += credits
            if status == "PASS":
                credits_earned += credits
                subjects_passed += 1
            else:
                subjects_failed += 1
                points = 0.0
            grade_point_sum += points * credits

        sgpa = round(grade_point_sum / credits_attempted, 2) if credits_attempted else 0.0
        return SGPAResult(
            sgpa=sgpa,
            credits_earned=credits_earned,
            credits_attempted=credits_attempted,
            subjects_passed=subjects_passed,
            subjects_failed=subjects_failed,
            grade_point_sum=grade_point_sum,
        )

    def compute_cgpa(self, semesters: list[SGPAResult]) -> CGPAResult:
        total_attempted = sum(item.credits_attempted for item in semesters)
        total_earned = sum(item.credits_earned for item in semesters)
        total_points = sum(item.grade_point_sum for item in semesters)
        return CGPAResult(
            cgpa=round(total_points / total_attempted, 2) if total_attempted else 0.0,
            semesters_completed=len(semesters),
            total_subjects_passed=sum(item.subjects_passed for item in semesters),
            total_subjects_failed=sum(item.subjects_failed for item in semesters),
            total_credits_earned=total_earned,
        )

    def compute_batch_statistics(self, cgpas: list[float]) -> BatchStatistics:
        if not cgpas:
            return BatchStatistics(0, 0.0, 0.0, 0.0, 0.0, {})

        ordered = sorted(float(value) for value in cgpas)
        total = len(ordered)
        avg = sum(ordered) / total
        variance = sum((value - avg) ** 2 for value in ordered) / total

        def percentile(p: int) -> float:
            if total == 1:
                return ordered[0]
            index = round((p / 100) * (total - 1))
            return ordered[index]

        return BatchStatistics(
            total_students=total,
            avg_cgpa=round(avg, 2),
            min_cgpa=ordered[0],
            max_cgpa=ordered[-1],
            std_cgpa=round(sqrt(variance), 4),
            percentiles={50: percentile(50), 75: percentile(75), 90: percentile(90)},
        )