from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional
import sys

_FORBIDDEN_PRODUCTION_CREDENTIALS = {
    "admin_secret_key_change_me", "readonly_secret_key_change_me",
    "admin_secret_key_here", "readonly_secret_key_here", "change_me",
    "changeme", "password", "secret",
}
_MIN_PRODUCTION_SECRET_LENGTH = 16

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")
    # Application Mode
    APP_MODE: str = "test"  # test | production

    # Database
    DATABASE_URL: str = "sqlite+aiosqlite:///./finctrl.db"

    # API Security
    ADMIN_API_KEY: Optional[str] = None
    READ_ONLY_API_KEY: Optional[str] = None
    CORS_ORIGINS: str = "http://localhost:5173"

    # AI Provider
    AI_PROVIDER: str = "gemini"
    GEMINI_API_KEY: Optional[str] = None
    GEMINI_MODEL: str = "gemini-2.5-flash"
    OPENROUTER_API_KEY: Optional[str] = None
    OPENROUTER_MODEL: str = "openrouter/free"
    OPENAI_API_KEY: Optional[str] = None

    # Razorpay
    RAZORPAY_MODE: str = "test"  # test | live
    RAZORPAY_KEY_ID: Optional[str] = None
    RAZORPAY_KEY_SECRET: Optional[str] = None
    RAZORPAY_WEBHOOK_SECRET: Optional[str] = None

    # Durable recovery leases (database-time seconds)
    RECONCILIATION_LEASE_SECONDS: int = 600
    RECONCILIATION_HEARTBEAT_SECONDS: int = 120
    AI_INVESTIGATION_LEASE_SECONDS: int = 300
    AI_INVESTIGATION_HEARTBEAT_SECONDS: int = 60
    WEBHOOK_LEASE_SECONDS: int = 120
    WEBHOOK_HEARTBEAT_SECONDS: int = 30
    RECOVERY_POLL_SECONDS: int = 15
    RECOVERY_BATCH_SIZE: int = 25

    @staticmethod
    def _unsafe_production_secret(value: Optional[str]) -> bool:
        if not value:
            return True
        normalized = value.strip().lower()
        return (len(value) < _MIN_PRODUCTION_SECRET_LENGTH
                or normalized in _FORBIDDEN_PRODUCTION_CREDENTIALS
                or normalized.startswith(("test_", "example_", "your_"))
                or "change_me" in normalized)

    def validate_production_config(self):
        """Fail fast if production mode is missing required configuration."""
        if self.APP_MODE != "production":
            return

        errors = []
        lease_pairs = (
            ("RECONCILIATION", self.RECONCILIATION_LEASE_SECONDS, self.RECONCILIATION_HEARTBEAT_SECONDS),
            ("AI_INVESTIGATION", self.AI_INVESTIGATION_LEASE_SECONDS, self.AI_INVESTIGATION_HEARTBEAT_SECONDS),
            ("WEBHOOK", self.WEBHOOK_LEASE_SECONDS, self.WEBHOOK_HEARTBEAT_SECONDS),
        )
        for name, lease, heartbeat in lease_pairs:
            if lease <= 0 or heartbeat <= 0 or heartbeat >= lease:
                errors.append(f"{name} heartbeat must be positive and shorter than its lease")
        if not 1 <= self.RECOVERY_POLL_SECONDS <= 300:
            errors.append("RECOVERY_POLL_SECONDS must be between 1 and 300")
        if not 1 <= self.RECOVERY_BATCH_SIZE <= 100:
            errors.append("RECOVERY_BATCH_SIZE must be between 1 and 100")

        # Database: Production requires PostgreSQL
        if not self.DATABASE_URL:
            errors.append("DATABASE_URL is required in production mode")
        elif not self.DATABASE_URL.startswith("postgresql"):
            errors.append("DATABASE_URL must be PostgreSQL in production mode (not SQLite)")

        # Security: Production requires API keys
        if self._unsafe_production_secret(self.ADMIN_API_KEY):
            errors.append("ADMIN_API_KEY must be an explicit non-placeholder secret of at least 16 characters")
        if self._unsafe_production_secret(self.READ_ONLY_API_KEY):
            errors.append("READ_ONLY_API_KEY must be an explicit non-placeholder secret of at least 16 characters")
        if self.ADMIN_API_KEY and self.READ_ONLY_API_KEY and self.ADMIN_API_KEY == self.READ_ONLY_API_KEY:
            errors.append("ADMIN_API_KEY and READ_ONLY_API_KEY must be different")

        # Razorpay: Production requires credentials
        if not self.RAZORPAY_KEY_ID:
            errors.append("RAZORPAY_KEY_ID is required in production mode")
        if not self.RAZORPAY_KEY_SECRET:
            errors.append("RAZORPAY_KEY_SECRET is required in production mode")
        if not self.RAZORPAY_WEBHOOK_SECRET:
            errors.append("RAZORPAY_WEBHOOK_SECRET is required in production mode")
        if (self.RAZORPAY_KEY_SECRET and self.RAZORPAY_WEBHOOK_SECRET
                and self.RAZORPAY_KEY_SECRET == self.RAZORPAY_WEBHOOK_SECRET):
            errors.append("RAZORPAY_WEBHOOK_SECRET must be different from RAZORPAY_KEY_SECRET")
        if self.RAZORPAY_MODE not in {"test", "live"}:
            errors.append("RAZORPAY_MODE must be either test or live")
        elif self.RAZORPAY_MODE != "live":
            errors.append("RAZORPAY_MODE must be live in production mode")
        elif self.RAZORPAY_KEY_ID and self.RAZORPAY_KEY_ID.startswith("rzp_test_"):
            errors.append("RAZORPAY_MODE=live cannot use a Razorpay test key ID")

        if self.AI_PROVIDER not in {"gemini", "openrouter", "openai"}:
            errors.append("AI_PROVIDER must be gemini, openrouter, or openai")

        if self.AI_PROVIDER == "gemini" and not self.GEMINI_API_KEY:
            errors.append("GEMINI_API_KEY is required when AI_PROVIDER=gemini")
        if self.AI_PROVIDER == "openrouter" and not self.OPENROUTER_API_KEY:
            errors.append("OPENROUTER_API_KEY is required when AI_PROVIDER=openrouter")
        # Legacy candidate investigation provider.
        if self.AI_PROVIDER == "openai" and not self.OPENAI_API_KEY:
            errors.append("OPENAI_API_KEY is required when AI_PROVIDER=openai")

        if errors:
            error_msg = "Production configuration validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
            print(error_msg, file=sys.stderr)
            sys.exit(1)

settings = Settings()

# Validate on import if in production mode
settings.validate_production_config()
