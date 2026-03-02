"""
Configuration management for AcadExtract.

Loads configuration from YAML files and environment variables.
Follows 12-factor app principles: config in environment, secrets via Vault/KMS.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _resolve_env_vars(config: dict[str, Any]) -> dict[str, Any]:
    """Recursively resolve ${VAR:-default} patterns in config values."""
    resolved = {}
    for key, value in config.items():
        if isinstance(value, dict):
            resolved[key] = _resolve_env_vars(value)
        elif isinstance(value, str) and value.startswith("${"):
            # Parse ${VAR:-default}
            inner = value[2:-1]
            if ":-" in inner:
                var_name, default = inner.split(":-", 1)
            else:
                var_name, default = inner, ""
            resolved[key] = os.environ.get(var_name, default)
        elif isinstance(value, list):
            resolved[key] = [
                _resolve_env_vars(item) if isinstance(item, dict) else item
                for item in value
            ]
        else:
            resolved[key] = value
    return resolved


def load_yaml_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """Load and resolve YAML configuration file."""
    if config_path is None:
        config_path = Path(__file__).parent.parent.parent / "config" / "system.yaml"
    config_path = Path(config_path)
    if not config_path.exists():
        return {}
    with open(config_path) as f:
        raw_config = yaml.safe_load(f)
    return _resolve_env_vars(raw_config or {})


class EmailConfig(BaseSettings):
    """Email ingestion configuration."""
    poll_interval_seconds: int = 60
    max_attachment_size_mb: int = 50
    dedup_simhash_threshold: float = 0.92
    classification_confidence_threshold: float = 0.85
    classification_review_threshold: float = 0.60
    max_retries: int = 3
    backoff_base_seconds: int = 2
    backoff_max_seconds: int = 60
    batch_size: int = 50


class DocumentConfig(BaseSettings):
    """Document parsing configuration."""
    ocr_confidence_threshold: float = 0.7
    table_detection_confidence: float = 0.5
    max_pages_per_document: int = 200
    image_dpi: int = 300
    deskew_angle_threshold_degrees: float = 0.5


class ExtractionConfig(BaseSettings):
    """Information extraction configuration."""
    llm_temperature: float = 0
    llm_max_tokens: int = 4096
    llm_model: str = "gpt-4o"
    max_validation_retries: int = 3
    gpa_max: float = 10.0
    marks_max_default: int = 100
    usn_pattern: str = r"[1-4][A-Z]{2}\d{2}[A-Z]{2,3}\d{3}"
    name_similarity_threshold: float = 0.92
    confidence_threshold_auto_accept: float = 0.85
    confidence_threshold_quarantine: float = 0.50


class AgentConfig(BaseSettings):
    """Agent orchestration configuration."""
    max_steps: int = 20
    planner_model: str = "gpt-4o"
    tool_timeout_seconds: int = 30
    circuit_breaker_threshold: int = 3
    circuit_breaker_window_seconds: int = 60
    memory_ttl_seconds: int = 3600
    reflection_enabled: bool = True
    max_concurrent_agents: int = 4


class QueryConfig(BaseSettings):
    """Query engine configuration."""
    intent_model: str = "gpt-4o"
    answer_model: str = "gpt-4o"
    sql_statement_timeout_seconds: int = 10
    max_results_per_query: int = 1000
    rate_limit_per_user_per_minute: int = 100
    rate_limit_per_institution_per_minute: int = 1000


class DatabaseConfig(BaseSettings):
    """Database connection configuration."""
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", populate_by_name=True)

    host: str = Field(default="localhost", alias="DB_HOST")
    port: int = Field(default=5432, alias="DB_PORT")
    name: str = Field(default="acadextract", alias="DB_NAME")
    user: str = Field(default="acadextract", alias="DB_USER")
    password: str = Field(default="", alias="DB_PASSWORD")
    pool_size: int = 20
    max_overflow: int = 10

    @property
    def async_url(self) -> str:
        return f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"

    @property
    def sync_url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"

    @property
    def url(self) -> str:
        """Default connection URL (async for asyncpg)."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


class RedisConfig(BaseSettings):
    """Redis configuration."""
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", populate_by_name=True)

    host: str = Field(default="localhost", alias="REDIS_HOST")
    port: int = Field(default=6379, alias="REDIS_PORT")
    password: str = Field(default="", alias="REDIS_PASSWORD")
    db: int = 0

    @property
    def url(self) -> str:
        auth = f":{self.password}@" if self.password else ""
        return f"redis://{auth}{self.host}:{self.port}/{self.db}"


class LLMConfig(BaseSettings):
    """LLM provider configuration."""
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", populate_by_name=True)

    primary_provider: str = "openai"
    primary_model: str = "gpt-4o"
    primary_api_key: str = Field(default="", alias="OPENAI_API_KEY")
    secondary_provider: str = "gemini"
    secondary_model: str = "gemini-1.5-pro"
    secondary_api_key: str = Field(default="", alias="GEMINI_API_KEY")
    embedding_model: str = "text-embedding-3-small"
    embedding_dimensions: int = 1536

    @property
    def providers(self) -> dict[str, dict[str, str]]:
        """Return provider config as a dict keyed by provider name."""
        return {
            "openai": {"api_key": self.primary_api_key, "model": self.primary_model},
            "google": {"api_key": self.secondary_api_key, "model": self.secondary_model},
        }


class SecurityConfig(BaseSettings):
    """Security configuration."""
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", populate_by_name=True)

    allowed_origins: list[str] = ["http://localhost:3000", "http://localhost:8000"]
    encryption_key: str = Field(default="", alias="ENCRYPTION_KEY")
    jwt_secret: str = Field(default="", alias="JWT_SECRET")


class Settings(BaseSettings):
    """Root settings aggregating all sub-configurations."""
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    environment: str = Field(default="development", alias="ENVIRONMENT")
    email: EmailConfig = EmailConfig()
    document: DocumentConfig = DocumentConfig()
    extraction: ExtractionConfig = ExtractionConfig()
    agent: AgentConfig = AgentConfig()
    query: QueryConfig = QueryConfig()
    database: DatabaseConfig = DatabaseConfig()
    redis: RedisConfig = RedisConfig()
    llm: LLMConfig = LLMConfig()
    security: SecurityConfig = SecurityConfig()


@lru_cache
def get_settings() -> Settings:
    """Get cached application settings singleton."""
    return Settings()
