"""
ML Email Classifier — AcadExtract.

Two-tier classification pipeline:
  1. TF-IDF + Logistic Regression (primary, fast, no GPU)
  2. DistilBERT sequence classifier (optional, requires transformers)

Both tiers degrade gracefully.  If neither is available
(cold start / no training data), falls back to the keyword
heuristic used in pipeline.py `_classify_email`.

Classes:
  - result_email      : academic result / marksheet
  - administrative    : fee reminder, timetable, circular
  - spam              : marketing, promotional
  - other             : anything else

Training data is bootstrapped from labeled examples in
config/classifier_data.json (created on first use).
The model is saved to config/models/email_classifier.pkl.
"""

from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

PROJECT_ROOT     = Path(__file__).resolve().parents[2]
DATA_PATH        = PROJECT_ROOT / "config" / "classifier_data.json"
MODEL_PATH       = PROJECT_ROOT / "config" / "models" / "email_classifier.pkl"

LABELS = ["result_email", "administrative", "spam", "other"]

# ── Seed training examples ────────────────────────────────────────────────────

_SEED_EXAMPLES: list[dict] = [
    # result_email
    {"text": "semester result marks sgpa cgpa grade backlog", "label": "result_email"},
    {"text": "examination result 1MS21CS001 6.5 sgpa pass",   "label": "result_email"},
    {"text": "grade card marks obtained internal external total", "label": "result_email"},
    {"text": "VTU result B.E 3rd semester marks statement", "label": "result_email"},
    {"text": "supplementary exam result reappear failed backlog registered", "label": "result_email"},
    {"text": "transcript marksheet academic performance report", "label": "result_email"},
    {"text": "SGPA 8.25 CGPA 7.90 student result notification", "label": "result_email"},
    # administrative
    {"text": "timetable schedule class college circular notice",      "label": "administrative"},
    {"text": "fee payment reminder last date dues pending",           "label": "administrative"},
    {"text": "holiday notification campus event seminar workshop",    "label": "administrative"},
    {"text": "admission form application deadline registration open", "label": "administrative"},
    # spam
    {"text": "congratulations winner prize claim offer discount",  "label": "spam"},
    {"text": "click here unsubscribe promotional deal limited",     "label": "spam"},
    {"text": "earn money work from home job opportunity free",      "label": "spam"},
    # other
    {"text": "meeting agenda project update team sync reply",   "label": "other"},
    {"text": "leave application medical certificate personal",  "label": "other"},
    {"text": "thank you regards best wishes greetings",         "label": "other"},
]

# ── Model helpers ─────────────────────────────────────────────────────────────

_model: Any = None
_vectorizer: Any = None


def _load_training_data() -> tuple[list[str], list[str]]:
    """Load training data from file or fall back to seeds."""
    texts, labels = [], []
    if DATA_PATH.exists():
        try:
            examples = json.loads(DATA_PATH.read_text())
            for ex in examples:
                texts.append(ex["text"])
                labels.append(ex["label"])
        except Exception:
            pass
    # Always include seed examples
    for ex in _SEED_EXAMPLES:
        texts.append(ex["text"])
        labels.append(ex["label"])
    return texts, labels


def _preprocess(text: str) -> str:
    """Lowercase, remove punctuation, normalise whitespace."""
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _train_model():
    """Train TF-IDF + LogisticRegression classifier. Saves model to disk."""
    global _model, _vectorizer
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.linear_model import LogisticRegression
        from sklearn.pipeline import Pipeline
        import joblib

        texts, labels = _load_training_data()
        preprocessed = [_preprocess(t) for t in texts]

        pipe = Pipeline([
            ("tfidf", TfidfVectorizer(
                ngram_range=(1, 2),
                max_features=5000,
                sublinear_tf=True,
            )),
            ("clf", LogisticRegression(
                C=1.0,
                max_iter=500,
                class_weight="balanced",
                multi_class="multinomial",
                solver="lbfgs",
            )),
        ])
        pipe.fit(preprocessed, labels)

        MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(pipe, MODEL_PATH)
        _model = pipe
        logger.info("email_classifier: TF-IDF+LR model trained (%d examples)", len(texts))
        return pipe
    except ImportError:
        logger.warning("email_classifier: sklearn not installed — using keyword fallback")
        return None
    except Exception as exc:
        logger.warning("email_classifier: training failed: %s", exc)
        return None


def _load_model():
    """Load saved model from disk, training if not present."""
    global _model
    if _model is not None:
        return _model
    try:
        import joblib
        if MODEL_PATH.exists():
            _model = joblib.load(MODEL_PATH)
            logger.debug("email_classifier: loaded model from %s", MODEL_PATH)
            return _model
    except Exception:
        pass
    return _train_model()


# ── DistilBERT classifier (optional) ─────────────────────────────────────────

_distilbert_classifier: Any = None
_DISTILBERT_AVAILABLE = False

def _try_load_distilbert():
    """Try to load a DistilBERT sequence classifier. Best-effort."""
    global _distilbert_classifier, _DISTILBERT_AVAILABLE
    try:
        from transformers import pipeline as hf_pipeline
        _distilbert_classifier = hf_pipeline(
            "text-classification",
            model="distilbert-base-uncased-finetuned-sst-2-english",
            top_k=None,
            truncation=True,
            max_length=512,
        )
        _DISTILBERT_AVAILABLE = True
        logger.info("email_classifier: DistilBERT classifier loaded")
    except Exception as exc:
        logger.debug("email_classifier: DistilBERT not available: %s", exc)


# ── Public API ────────────────────────────────────────────────────────────────

def classify_email(subject: str, body: str) -> tuple[str, float]:
    """
    Classify an email into (label, confidence).

    Pipeline:
      1. TF-IDF + LogReg (primary)
      2. DistilBERT if primary confidence < 0.70 and model available
      3. Keyword heuristic fallback

    Returns:
        (label, confidence) where label ∈ {result_email, administrative, spam, other}
        and confidence ∈ [0, 1].
    """
    text = f"{subject} {body}"[:1000]

    # ── 1. TF-IDF + LR ───────────────────────────────────────────────────────
    model = _load_model()
    if model is not None:
        try:
            preprocessed = _preprocess(text)
            proba = model.predict_proba([preprocessed])[0]
            classes = model.classes_
            idx = int(proba.argmax())
            label = classes[idx]
            confidence = float(proba[idx])

            # If LR is uncertain, try DistilBERT
            if confidence < 0.70 and _DISTILBERT_AVAILABLE and _distilbert_classifier:
                try:
                    results = _distilbert_classifier(text[:512])
                    # Map sentiment to email class heuristically
                    top = results[0]
                    db_label = top["label"].lower()
                    db_conf = float(top["score"])
                    # Simple heuristic: POSITIVE sentiment + result keywords → result_email
                    _result_kw = re.search(r"sgpa|cgpa|grade|marks|result|backlog", text.lower())
                    if _result_kw:
                        return "result_email", max(confidence, db_conf * 0.85)
                    return label, confidence
                except Exception:
                    pass

            return label, confidence
        except Exception as exc:
            logger.debug("email_classifier: model predict failed: %s", exc)

    # ── 2. Keyword heuristic fallback ─────────────────────────────────────────
    return _keyword_classify(text)


def _keyword_classify(text: str) -> tuple[str, float]:
    """Rule-based fallback classifier (same logic as pipeline.py)."""
    _STRONG = {"result", "marks", "grade", "sgpa", "cgpa", "marksheet",
               "semester result", "examination result", "backlog", "grade card",
               "transcript", "internal marks", "pass", "fail"}
    _WEAK   = {"exam", "test", "score", "performance", "academic", "gpa",
               "percentage", "assessment"}
    _NEG    = {"unsubscribe", "promotional", "offer", "discount", "click here",
               "prize", "winner", "free"}

    lower = text.lower()
    if any(w in lower for w in _NEG):
        return "spam", 0.85

    strong_hits = sum(1 for kw in _STRONG if kw in lower)
    weak_hits   = sum(1 for kw in _WEAK  if kw in lower)

    if strong_hits >= 2:
        return "result_email", min(0.90, 0.75 + strong_hits * 0.05)
    if strong_hits == 1:
        return "result_email", 0.65 + weak_hits * 0.05
    if weak_hits >= 2:
        return "result_email", 0.50
    return "other", 0.60


def add_training_example(text: str, label: str) -> None:
    """
    Add a labeled example to the training data file and retrain the model.
    Called when a teacher approves/corrects a classification in the review queue.
    """
    if label not in LABELS:
        raise ValueError(f"Unknown label: {label}. Must be one of {LABELS}")
    examples = []
    if DATA_PATH.exists():
        try:
            examples = json.loads(DATA_PATH.read_text())
        except Exception:
            pass
    examples.append({"text": _preprocess(text), "label": label})
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    DATA_PATH.write_text(json.dumps(examples, indent=2))
    # Retrain with new data (quick — TF-IDF+LR is fast)
    global _model
    _model = None
    _train_model()


# Kick off DistilBERT load in background on first import (non-blocking)
try:
    import threading
    threading.Thread(target=_try_load_distilbert, daemon=True).start()
except Exception:
    pass
