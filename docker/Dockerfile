# ── Stage 1: Builder ────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# System dependencies for OCR and PDF processing
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    libgl1-mesa-glx \
    libglib2.0-0 \
    tesseract-ocr \
    tesseract-ocr-eng \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir --prefix=/install .

# ── Stage 2: Runtime ───────────────────────────────────────────────
FROM python:3.11-slim AS runtime

WORKDIR /app

# Runtime system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    libgl1-mesa-glx \
    libglib2.0-0 \
    tesseract-ocr \
    tesseract-ocr-eng \
    poppler-utils \
    clamav-daemon \
    && rm -rf /var/lib/apt/lists/*

# Copy installed Python packages
COPY --from=builder /install /usr/local

# Copy application code
COPY src/ ./src/
COPY config/ ./config/
COPY schemas/ ./schemas/

# Non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser
RUN chown -R appuser:appuser /app
USER appuser

# Environment
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV APP_ENV=production

EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/healthz')"

# Default: run API server
CMD ["uvicorn", "src.api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
