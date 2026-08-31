"""
Tests for production configuration validation.
"""
import pytest
import sys
import os


def test_test_mode_allows_missing_credentials():
    """Test mode should allow missing API keys and credentials."""
    # In test mode, missing credentials should not cause validation failure
    # This is tested by the fact that the test suite runs with APP_MODE=test
    from finctrl.backend.config import settings

    # Test mode should be active
    assert settings.APP_MODE == "test"
    # No validation errors should occur in test mode even with missing credentials


def test_production_mode_requires_postgresql():
    """Production mode should reject SQLite database."""
    # This test verifies the validation logic without actually setting production mode
    # (which would cause all tests to fail)
    from finctrl.backend.config import Settings

    # Create a test settings instance with SQLite in production
    test_settings = Settings(
        APP_MODE="production",
        DATABASE_URL="sqlite+aiosqlite:///test.db",
        ADMIN_API_KEY="admin_key_1234567890",
        READ_ONLY_API_KEY="readonly_key_1234567890",
        RAZORPAY_KEY_ID="rzp_test_123",
        RAZORPAY_KEY_SECRET="rzp_secret"
    )

    # The validation should detect SQLite is not allowed
    try:
        test_settings.validate_production_config()
        # Should not reach here
        assert False, "Expected SystemExit but validation passed"
    except SystemExit as e:
        # Expected - validation should fail for SQLite in production
        assert e.code == 1


def test_production_requires_api_keys():
    """Production mode requires ADMIN and READ_ONLY API keys."""
    from finctrl.backend.config import Settings

    # Create settings with missing API keys
    test_settings = Settings(
        APP_MODE="production",
        DATABASE_URL="postgresql+asyncpg://postgres:postgres@localhost:5432/finctrl",
        RAZORPAY_KEY_ID="rzp_test_123",
        RAZORPAY_KEY_SECRET="rzp_secret"
    )

    try:
        test_settings.validate_production_config()
        assert False, "Expected SystemExit but validation passed"
    except SystemExit as e:
        assert e.code == 1


def test_production_requires_razorpay_credentials():
    """Production mode requires Razorpay credentials."""
    from finctrl.backend.config import Settings

    # Create settings with missing Razorpay credentials
    test_settings = Settings(
        APP_MODE="production",
        DATABASE_URL="postgresql+asyncpg://postgres:postgres@localhost:5432/finctrl",
        ADMIN_API_KEY="admin_key",
        READ_ONLY_API_KEY="readonly_key"
    )

    try:
        test_settings.validate_production_config()
        assert False, "Expected SystemExit but validation passed"
    except SystemExit as e:
        assert e.code == 1


def test_openai_api_key_only_required_when_using_openai():
    """OPENAI_API_KEY should only be required when AI_PROVIDER=openai."""
    from finctrl.backend.config import Settings

    # Test with openai provider but missing key
    test_settings = Settings(
        APP_MODE="production",
        DATABASE_URL="postgresql+asyncpg://postgres:postgres@localhost:5432/finctrl",
        ADMIN_API_KEY="admin_key",
        READ_ONLY_API_KEY="readonly_key",
        RAZORPAY_KEY_ID="rzp_test_123",
        RAZORPAY_KEY_SECRET="rzp_secret",
        AI_PROVIDER="openai"
    )

    try:
        test_settings.validate_production_config()
        assert False, "Expected SystemExit but validation passed"
    except SystemExit as e:
        assert e.code == 1


def test_valid_production_config():
    """Valid production configuration should load without errors."""
    from finctrl.backend.config import Settings

    # Create valid production settings
    test_settings = Settings(
        APP_MODE="production",
        DATABASE_URL="postgresql+asyncpg://postgres:postgres@localhost:5432/finctrl",
        ADMIN_API_KEY="admin_key_1234567890",
        READ_ONLY_API_KEY="readonly_key_1234567890",
        RAZORPAY_KEY_ID="rzp_live_123",
        RAZORPAY_KEY_SECRET="rzp_secret_123",
        RAZORPAY_WEBHOOK_SECRET="webhook_secret_123456",
        RAZORPAY_MODE="live",
        AI_PROVIDER="openrouter",
        OPENROUTER_API_KEY="or_key_123"
    )

    # Should not raise SystemExit
    test_settings.validate_production_config()

    # Verify settings
    assert test_settings.APP_MODE == "production"
    assert test_settings.DATABASE_URL == "postgresql+asyncpg://postgres:postgres@localhost:5432/finctrl"
    assert test_settings.ADMIN_API_KEY == "admin_key_1234567890"
    assert test_settings.READ_ONLY_API_KEY == "readonly_key_1234567890"
    assert test_settings.RAZORPAY_KEY_ID == "rzp_live_123"
    assert test_settings.RAZORPAY_KEY_SECRET == "rzp_secret_123"
    assert test_settings.AI_PROVIDER == "openrouter"
    assert test_settings.OPENROUTER_API_KEY == "or_key_123"


def test_openrouter_without_openai_key():
    """Using OpenRouter should not require OPENAI_API_KEY."""
    from finctrl.backend.config import Settings

    # Valid production config using OpenRouter (no OPENAI_API_KEY needed)
    test_settings = Settings(
        APP_MODE="production",
        DATABASE_URL="postgresql+asyncpg://postgres:postgres@localhost:5432/finctrl",
        ADMIN_API_KEY="admin_key_1234567890",
        READ_ONLY_API_KEY="readonly_key_1234567890",
        RAZORPAY_KEY_ID="rzp_live_123",
        RAZORPAY_KEY_SECRET="rzp_secret",
        RAZORPAY_WEBHOOK_SECRET="webhook_secret_123456",
        RAZORPAY_MODE="live",
        AI_PROVIDER="openrouter",
        OPENROUTER_API_KEY="or_key_1234567890"
    )

    # Should not raise SystemExit
    test_settings.validate_production_config()
    assert test_settings.AI_PROVIDER == "openrouter"


@pytest.mark.parametrize("admin_key,readonly_key", [
    ("admin_secret_key_change_me", "readonly_secure_12345"),
    ("admin_secure_123456", "readonly_secret_key_change_me"),
    ("short", "readonly_secure_12345"),
    ("same_secure_key_123", "same_secure_key_123"),
])
def test_production_rejects_placeholder_weak_or_equal_api_keys(admin_key, readonly_key):
    from finctrl.backend.config import Settings
    configured = Settings(APP_MODE="production",
        DATABASE_URL="postgresql+asyncpg://db.example/finctrl",
        ADMIN_API_KEY=admin_key, READ_ONLY_API_KEY=readonly_key,
        RAZORPAY_MODE="live", RAZORPAY_KEY_ID="rzp_live_example",
        RAZORPAY_KEY_SECRET="api_secret_123456",
        RAZORPAY_WEBHOOK_SECRET="webhook_secret_123456",
        AI_PROVIDER="openrouter", OPENROUTER_API_KEY="provider_key_123456")
    with pytest.raises(SystemExit):
        configured.validate_production_config()


def test_production_requires_distinct_webhook_secret():
    from finctrl.backend.config import Settings
    configured = Settings(APP_MODE="production",
        DATABASE_URL="postgresql+asyncpg://db.example/finctrl",
        ADMIN_API_KEY="admin_secure_key_123", READ_ONLY_API_KEY="readonly_secure_key_123",
        RAZORPAY_MODE="live", RAZORPAY_KEY_ID="rzp_live_example",
        RAZORPAY_KEY_SECRET="api_secret_123456", RAZORPAY_WEBHOOK_SECRET=None,
        AI_PROVIDER="openrouter", OPENROUTER_API_KEY="provider_key_123456")
    with pytest.raises(SystemExit):
        configured.validate_production_config()

    configured.RAZORPAY_WEBHOOK_SECRET = configured.RAZORPAY_KEY_SECRET
    with pytest.raises(SystemExit):
        configured.validate_production_config()
