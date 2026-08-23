from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    AI_PROVIDER: str = "openrouter"
    OPENROUTER_API_KEY: Optional[str] = None
    OPENROUTER_MODEL: str = "openrouter/free"
    OPENAI_API_KEY: Optional[str] = None
    RAZORPAY_KEY_ID: Optional[str] = None
    RAZORPAY_KEY_SECRET: Optional[str] = None

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()
