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
    msgraph: MSGraphConfig = MSGraphConfig()
    webhook: WebhookConfig = WebhookConfig()
    database: DatabaseConfig = DatabaseConfig()
    redis: RedisConfig = RedisConfig()
    llm: LLMConfig = LLMConfig()
    security: SecurityConfig = SecurityConfig()


@lru_cache
def get_settings() -> Settings:
    """Get cached application settings singleton."""
    return Settings()
