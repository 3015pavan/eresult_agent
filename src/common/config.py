"""
Configuration management for AcadExtract.

Loads environment variables via pydantic-settings.
12-factor: all config via environment / .env file.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MSGraphConfig(BaseSettings):
    """Microsoft Graph API config for Office 365 / Exchange Online."""
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", populate_by_name=True)

    tenant_id: str = Field(default="", alias="MSGRAPH_TENANT_ID")
    client_id: str = Field(default="", alias="MSGRAPH_CLIENT_ID")
    client_secret: str = Field(default="", alias="MSGRAPH_CLIENT_SECRET")
    user_email: str = Field(default="", alias="MSGRAPH_USER_EMAIL")
    graph_endpoint: str = "https://graph.microsoft.com/v1.0"
    authority_url_template: str = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    subscription_notification_url: str = Field(default="", alias="MSGRAPH_WEBHOOK_URL")
    subscription_expiry_minutes: int = Field(default=4230, alias="MSGRAPH_SUB_EXPIRY_MINUTES")


class WebhookConfig(BaseSettings):
    """Inbound webhook config for SMTP-relay services (Mailgun, SendGrid, Postmark)."""
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", populate_by_name=True)

    # HMAC-SHA256 secret shared with the relay provider
    secret_token: str = Field(default="", alias="WEBHOOK_SECRET_TOKEN")
    # Redis list key where webhook handler pushes raw email bytes
    queue_key: str = Field(default="webhook:email:queue", alias="WEBHOOK_QUEUE_KEY")
    signature_header: str = "X-Webhook-Signature"


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
        """Synchronous connection URL (same as sync_url, for psycopg2)."""
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
    secondary_model: str = Field(default="gemini-2.0-flash", alias="GEMINI_MODEL")
    secondary_api_key: str = Field(default="", alias="GEMINI_API_KEY")
    groq_api_key: str = Field(default="", alias="GROQ_API_KEY")
    groq_model: str = Field(default="llama-3.3-70b-versatile", alias="GROQ_MODEL")
    embedding_model: str = "text-embedding-3-small"
    embedding_dimensions: int = 1536

    @property
    def active_provider(self) -> str:
        """Return whichever provider has a key configured. Groq > OpenAI > Gemini."""
        if self.groq_api_key:
            return "groq"
        if self.primary_api_key:
            return "openai"
        if self.secondary_api_key:
            return "gemini"
        return "none"

    @property
    def active_model(self) -> str:
        if self.groq_api_key:
            return self.groq_model
        if self.primary_api_key:
            return self.primary_model
        return self.secondary_model

    @property
    def active_api_key(self) -> str:
        if self.groq_api_key:
            return self.groq_api_key
        if self.primary_api_key:
            return self.primary_api_key
        return self.secondary_api_key

    @property
    def providers(self) -> dict[str, dict[str, str]]:
        """Return provider config as a dict keyed by provider name."""
        return {
            "openai": {"api_key": self.primary_api_key, "model": self.primary_model},
            "google": {"api_key": self.secondary_api_key, "model": self.secondary_model},
            "groq": {"api_key": self.groq_api_key, "model": self.groq_model},
        }


class SMTPConfig(BaseSettings):
    """SMTP email-sending configuration."""
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", populate_by_name=True)

    host: str = Field(default="smtp.gmail.com", alias="SMTP_HOST")
    port: int = Field(default=587, alias="SMTP_PORT")
    user: str = Field(default="", alias="SMTP_USER")
    password: str = Field(default="", alias="SMTP_PASSWORD")
    from_name: str = Field(default="AcadExtract", alias="SMTP_FROM_NAME")
    use_tls: bool = Field(default=True, alias="SMTP_USE_TLS")

    @property
    def configured(self) -> bool:
        return bool(self.user and self.password)


class StorageConfig(BaseSettings):
    """Object storage configuration with MinIO or Supabase support."""
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", populate_by_name=True)

    backend: str = Field(default="minio", alias="STORAGE_BACKEND")
    minio_endpoint: str = Field(default="localhost:9000", alias="MINIO_ENDPOINT")
    minio_access_key: str = Field(default="minioadmin", alias="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field(default="minioadmin123", alias="MINIO_SECRET_KEY")
    minio_secure: bool = Field(default=False, alias="MINIO_SECURE")
    minio_bucket_emails: str = Field(default="emails-raw", alias="MINIO_BUCKET_EMAILS")
    minio_bucket_attachments: str = Field(default="attachments", alias="MINIO_BUCKET_ATTACHMENTS")
    supabase_url: str = Field(default="", alias="SUPABASE_URL")
    supabase_service_key: str = Field(default="", alias="SUPABASE_SERVICE_KEY")
    supabase_bucket_emails: str = Field(default="emails-raw", alias="SUPABASE_BUCKET_EMAILS")
    supabase_bucket_attachments: str = Field(default="attachments", alias="SUPABASE_BUCKET_ATTACHMENTS")

    @property
    def supabase_configured(self) -> bool:
        return bool(self.supabase_url and self.supabase_service_key)


class DocumentAIConfig(BaseSettings):
    """Optional document AI integrations for richer parsing."""
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", populate_by_name=True)

    yolo_enabled: bool = Field(default=False, alias="YOLO_ENABLED")
    yolo_model_path: str = Field(default="", alias="YOLO_MODEL_PATH")
    yolo_confidence: float = Field(default=0.25, alias="YOLO_CONFIDENCE")
    llamaparse_enabled: bool = Field(default=False, alias="LLAMAPARSE_ENABLED")
    llamaparse_api_key: str = Field(default="", alias="LLAMAPARSE_API_KEY")
    llamaparse_result_type: str = Field(default="markdown", alias="LLAMAPARSE_RESULT_TYPE")
    aws_ses_enabled: bool = Field(default=False, alias="AWS_SES_ENABLED")
    aws_region: str = Field(default="ap-south-1", alias="AWS_REGION")
    aws_access_key_id: str = Field(default="", alias="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str = Field(default="", alias="AWS_SECRET_ACCESS_KEY")


class SecurityConfig(BaseSettings):
    """Security configuration."""
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", populate_by_name=True)

    allowed_origins: list[str] = ["http://localhost:3000", "http://localhost:8000", "http://localhost:8002", "http://127.0.0.1:8002"]
    app_api_key: str = Field(default="", alias="APP_API_KEY")
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
    sentry_dsn: str = Field(default="", alias="SENTRY_DSN")
    msgraph: MSGraphConfig = MSGraphConfig()
    webhook: WebhookConfig = WebhookConfig()
    database: DatabaseConfig = DatabaseConfig()
    redis: RedisConfig = RedisConfig()
    llm: LLMConfig = LLMConfig()
    smtp: SMTPConfig = SMTPConfig()
    storage: StorageConfig = StorageConfig()
    document_ai: DocumentAIConfig = DocumentAIConfig()
    security: SecurityConfig = SecurityConfig()


@lru_cache
def get_settings() -> Settings:
    """Get cached application settings singleton."""
    return Settings()
