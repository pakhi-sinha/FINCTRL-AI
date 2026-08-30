from pydantic_settings import BaseSettings
from typing import Optional
import sys

class Settings(BaseSettings):
    # Application Mode
    APP_MODE: str = "test"  # test | production

    # Database
    DATABASE_URL: Optional[str] = None

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

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    def validate_production_config(self):
        """Fail fast if production mode is missing required configuration."""
        if self.APP_MODE != "production":
            return

        errors = []

        # Database: Production requires PostgreSQL
        if not self.DATABASE_URL:
            errors.append("DATABASE_URL is required in production mode")
        elif not self.DATABASE_URL.startswith("postgresql"):
            errors.append("DATABASE_URL must be PostgreSQL in production mode (not SQLite)")

        # Security: Production requires API keys
        if not self.ADMIN_API_KEY or self.ADMIN_API_KEY.startswith("test_"):
            errors.append("ADMIN_API_KEY is required in production mode")
        if not self.READ_ONLY_API_KEY or self.READ_ONLY_API_KEY.startswith("test_"):
            errors.append("READ_ONLY_API_KEY is required in production mode")

        # Razorpay: Production requires credentials
        if not self.RAZORPAY_KEY_ID:
            errors.append("RAZORPAY_KEY_ID is required in production mode")
        if not self.RAZORPAY_KEY_SECRET:
            errors.append("RAZORPAY_KEY_SECRET is required in production mode")

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
