"""
Extraction Merger.

Fuses results from multiple extraction strategies (rule-based, regex, LLM)
into a single consolidated set of student records.

Voting strategies:
  1. All agree → confidence 0.98 (highest trust)
  2. Majority agree → confidence 0.85
  3. Conflict → prefer rule-based first, regex second, LLM third
  4. Single source → use as-is with original confidence

Field-level merging: Each field has independent confidence tracking.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from src.common.models import (
    StudentRecord,
    SubjectResult,
    ResultStatus,
    ExtractionStrategy,
)
from src.common.observability import get_logger

logger = get_logger(__name__)


# Strategy priority (lower = higher trust for deterministic fields)
STRATEGY_PRIORITY = {
    ExtractionStrategy.RULE_BASED: 1,
    ExtractionStrategy.REGEX: 2,
    ExtractionStrategy.LLM: 3,
}

# Strategy weights for confidence aggregation
STRATEGY_WEIGHT = {
    ExtractionStrategy.RULE_BASED: 0.40,
    ExtractionStrategy.REGEX: 0.25,
    ExtractionStrategy.LLM: 0.35,
}


class ExtractionMerger:
    """
    Merge student records from multiple extraction strategies.

    Merging algorithm:
      1. Group records by USN across all strategies
      2. For each USN, merge field-by-field
      3. Use voting for categorical fields (status, grade)
      4. Use weighted average for numeric fields (marks, GPA)
      5. Prefer highest-confidence source for text fields (name)
      6. Compute final confidence based on agreement level
    """

    def merge(
        self,
        strategy_results: dict[ExtractionStrategy, list[StudentRecord]],
    ) -> list[StudentRecord]:
        """
        Merge results from all strategies.

        Args:
            strategy_results: Map of strategy → extracted records

        Returns:
            Merged, deduplicated list of StudentRecords
        """
        if not strategy_results:
            return []

        # If only one strategy produced results, return those
        non_empty = {
            k: v for k, v in strategy_results.items() if v
        }
        if len(non_empty) == 1:
            strategy, records = next(iter(non_empty.items()))
            logger.info(
                "single_strategy_merge",
                strategy=strategy.value,
                records=len(records),
            )
            return records

        # Group all records by USN
        usn_groups: dict[str, dict[ExtractionStrategy, StudentRecord]] = (
            defaultdict(dict)
        )

        for strategy, records in strategy_results.items():
            for record in records:
                usn_groups[record.usn][strategy] = record

        # Merge each USN group
        merged: list[StudentRecord] = []
        for usn, sources in usn_groups.items():
            merged_record = self._merge_usn_group(usn, sources)
            if merged_record:
                merged.append(merged_record)

        logger.info(
            "merge_complete",
            total_records=len(merged),
            strategies_used=list(strategy_results.keys()),
            usn_groups=len(usn_groups),
        )

        return merged

    def _merge_usn_group(
        self,
        usn: str,
        sources: dict[ExtractionStrategy, StudentRecord],
    ) -> StudentRecord | None:
        """Merge records for a single USN from multiple strategies."""
        if not sources:
            return None

        if len(sources) == 1:
            return next(iter(sources.values()))

        # Determine agreement level for global confidence
        num_strategies = len(sources)
        agreement = self._compute_agreement(sources)

        # Merge each field independently
        name = self._merge_name(sources)
        sgpa = self._merge_sgpa(sources)
        subjects = self._merge_subjects(sources)
        field_confidences = self._merge_field_confidences(sources)

        # Compute overall confidence based on agreement
        if agreement == "full":
            overall_confidence = 0.98
        elif agreement == "majority":
            overall_confidence = 0.85
        else:
            overall_confidence = 0.70

        # Select the "best" strategy label
        best_strategy = min(
            sources.keys(),
            key=lambda s: STRATEGY_PRIORITY.get(s, 99),
        )

        return StudentRecord(
            usn=usn,
            name=name,
            subjects=subjects,
            sgpa=sgpa,
            extraction_strategy=best_strategy,
            overall_confidence=overall_confidence,
            field_confidences=field_confidences,
        )

    def _compute_agreement(
        self,
        sources: dict[ExtractionStrategy, StudentRecord],
    ) -> str:
        """
        Compute agreement level across strategies.

        Returns: "full" | "majority" | "conflict"
        """
        records = list(sources.values())

        # Check SGPA agreement (if available)
        sgpas = [r.sgpa for r in records if r.sgpa is not None]
        sgpa_agree = True
        if len(sgpas) >= 2:
            sgpa_agree = max(sgpas) - min(sgpas) <= 0.5

        # Check subject count agreement
        subject_counts = [len(r.subjects) for r in records]
        count_agree = max(subject_counts) - min(subject_counts) <= 1

        # Check name agreement
        names = [r.name.lower().strip() for r in records if r.name]
        name_agree = len(set(names)) <= 1

        if sgpa_agree and count_agree and name_agree:
            return "full"
        elif sum([sgpa_agree, count_agree, name_agree]) >= 2:
            return "majority"
        else:
            return "conflict"

    def _merge_name(
        self,
        sources: dict[ExtractionStrategy, StudentRecord],
    ) -> str | None:
        """Merge name field — prefer highest-confidence source."""
        candidates: list[tuple[float, str, ExtractionStrategy]] = []

        for strategy, record in sources.items():
            if record.name:
                conf = record.field_confidences.get("name", 0.5)
                candidates.append((conf, record.name, strategy))

        if not candidates:
            return None

        # Sort by confidence, then by strategy priority
        candidates.sort(
            key=lambda x: (-x[0], STRATEGY_PRIORITY.get(x[2], 99)),
        )
        return candidates[0][1]

    def _merge_sgpa(
        self,
        sources: dict[ExtractionStrategy, StudentRecord],
    ) -> float | None:
        """Merge SGPA — weighted average if similar, else best source."""
        candidates: list[tuple[float, float, ExtractionStrategy]] = []

        for strategy, record in sources.items():
            if record.sgpa is not None:
                conf = record.field_confidences.get("sgpa", 0.5)
                candidates.append((record.sgpa, conf, strategy))

        if not candidates:
            return None

        if len(candidates) == 1:
            return candidates[0][0]

        # Check if values are close (within 0.5)
        values = [c[0] for c in candidates]
        if max(values) - min(values) <= 0.5:
            # Weighted average
            total_weight = 0.0
            weighted_sum = 0.0
            for sgpa, conf, strategy in candidates:
                weight = conf * STRATEGY_WEIGHT.get(strategy, 0.25)
                weighted_sum += sgpa * weight
                total_weight += weight
            return round(weighted_sum / total_weight, 2) if total_weight > 0 else values[0]
        else:
            # Values diverge — pick highest confidence
            candidates.sort(
                key=lambda x: (-x[1], STRATEGY_PRIORITY.get(x[2], 99)),
            )
            return candidates[0][0]

    def _merge_subjects(
        self,
        sources: dict[ExtractionStrategy, StudentRecord],
    ) -> list[SubjectResult]:
        """
        Merge subject results across strategies.

        Strategy:
          1. Group subjects by subject_code
          2. For each group, merge marks/grade/status
          3. Subjects without codes are matched by position
        """
        # Group subjects by code
        code_groups: dict[str | None, dict[ExtractionStrategy, SubjectResult]] = (
            defaultdict(dict)
        )

        for strategy, record in sources.items():
            for subj in record.subjects:
                key = subj.subject_code or f"_pos_{record.subjects.index(subj)}"
                code_groups[key][strategy] = subj

        merged_subjects: list[SubjectResult] = []

        for code, subj_sources in code_groups.items():
            merged_subj = self._merge_single_subject(code, subj_sources)
            if merged_subj:
                merged_subjects.append(merged_subj)

        return merged_subjects

    def _merge_single_subject(
        self,
        code: str | None,
        sources: dict[ExtractionStrategy, SubjectResult],
    ) -> SubjectResult | None:
        """Merge a single subject result from multiple strategies."""
        if not sources:
            return None

        if len(sources) == 1:
            return next(iter(sources.values()))

        subjects = list(sources.values())
        strategies = list(sources.keys())

        # Subject code — prefer non-None
        subject_code = None
        for s in subjects:
            if s.subject_code:
                subject_code = s.subject_code
                break

        # Subject name — prefer non-None, longest
        subject_name = None
        for s in sorted(subjects, key=lambda x: len(x.subject_name or ""), reverse=True):
            if s.subject_name:
                subject_name = s.subject_name
                break

        # Marks — weighted average if close, else best
        marks_candidates = [
            (s.total_marks, strategies[i])
            for i, s in enumerate(subjects)
            if s.total_marks is not None
        ]
        total_marks = None
        if marks_candidates:
            marks_values = [m[0] for m in marks_candidates]
            if max(marks_values) - min(marks_values) <= 5:
                total_marks = round(sum(marks_values) / len(marks_values))
            else:
                # Pick from highest priority strategy
                marks_candidates.sort(
                    key=lambda x: STRATEGY_PRIORITY.get(x[1], 99),
                )
                total_marks = marks_candidates[0][0]

        # Max marks — take max (should be consistent)
        max_marks_values = [s.max_marks for s in subjects if s.max_marks]
        max_marks = max(max_marks_values) if max_marks_values else None

        # Grade — majority vote
        grade = self._majority_vote(
            [s.grade for s in subjects if s.grade],
        )

        # Status — majority vote
        status_values = [s.status for s in subjects if s.status]
        status = self._majority_vote(status_values) or ResultStatus.PASS

        return SubjectResult(
            subject_code=subject_code,
            subject_name=subject_name,
            total_marks=total_marks,
            max_marks=max_marks,
            grade=grade,
            status=status,
        )

    def _merge_field_confidences(
        self,
        sources: dict[ExtractionStrategy, StudentRecord],
    ) -> dict[str, float]:
        """Merge field-level confidences via weighted max."""
        all_fields: set[str] = set()
        for record in sources.values():
            all_fields.update(record.field_confidences.keys())

        merged: dict[str, float] = {}
        for field in all_fields:
            values: list[tuple[float, float]] = []
            for strategy, record in sources.items():
                if field in record.field_confidences:
                    weight = STRATEGY_WEIGHT.get(strategy, 0.25)
                    values.append((record.field_confidences[field], weight))

            if values:
                # Weighted maximum
                total = sum(v * w for v, w in values)
                total_weight = sum(w for _, w in values)
                merged[field] = round(total / total_weight, 3) if total_weight > 0 else 0.0

        return merged

    @staticmethod
    def _majority_vote(candidates: list[Any]) -> Any | None:
        """Return the most common value, or None if empty."""
        if not candidates:
            return None

        counts: dict[Any, int] = defaultdict(int)
        for c in candidates:
            counts[c] += 1

        return max(counts, key=counts.get)
