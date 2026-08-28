from pydantic_settings import BaseSettings
from typing import Optional
from pydantic import Field, field_validator
import os

class Settings(BaseSettings):
    APP_MODE: str = Field("test", description="Application mode: test or production")

    # AI Config
    AI_PROVIDER: str = "openrouter"
    OPENROUTER_API_KEY: Optional[str] = None
    OPENROUTER_MODEL: str = "openrouter/free"
    OPENAI_API_KEY: Optional[str] = None

    # Razorpay Config
    RAZORPAY_MODE: str = "test"
    RAZORPAY_KEY_ID: Optional[str] = None
    RAZORPAY_KEY_SECRET: Optional[str] = None
    RAZORPAY_WEBHOOK_SECRET: Optional[str] = None

    # Database Config
    DATABASE_URL: Optional[str] = None

    # Security Config
    ADMIN_API_KEY: Optional[str] = None
    READ_ONLY_API_KEY: Optional[str] = None

    @field_validator("APP_MODE")
    @classmethod
    def validate_app_mode(cls, v):
        if v not in ("test", "production"):
            raise ValueError("APP_MODE must be either 'test' or 'production'")
        return v

    def validate_production(self):
        """Enforces required secrets in production mode (fail-fast)"""
        if self.APP_MODE == "production":
            missing = []
            if not self.DATABASE_URL:
                missing.append("DATABASE_URL")
            if not self.RAZORPAY_KEY_ID:
                missing.append("RAZORPAY_KEY_ID")
            if not self.RAZORPAY_KEY_SECRET:
                missing.append("RAZORPAY_KEY_SECRET")
            if not self.RAZORPAY_WEBHOOK_SECRET:
                missing.append("RAZORPAY_WEBHOOK_SECRET")
            if not self.ADMIN_API_KEY:
                missing.append("ADMIN_API_KEY")
            if not self.READ_ONLY_API_KEY:
                missing.append("READ_ONLY_API_KEY")

            if missing:
                raise ValueError(f"Missing required production configuration: {', '.join(missing)}")

            if self.DATABASE_URL and self.DATABASE_URL.startswith("sqlite"):
                raise ValueError("SQLite is not allowed in production mode")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"

settings = Settings()
settings.validate_production()
