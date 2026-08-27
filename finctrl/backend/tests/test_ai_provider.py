import os
os.environ['DATABASE_URL'] = 'sqlite+aiosqlite:///:memory:'

import pytest
from finctrl.backend.engine.ai.provider import OpenRouterProvider, OpenAIProvider, MockAIProvider, get_ai_provider
from finctrl.backend.config import settings

@pytest.mark.asyncio
async def test_mock_provider():
    from openai.types.chat.chat_completion_message import ChatCompletionMessage
    provider = MockAIProvider()

    # Needs a mock response
    provider.next_message = ChatCompletionMessage(
        content='{"decision": "PROPOSE_MATCH", "match_type": "ONE_TO_ONE", "evidence_ids": ["123"], "confidence": 0.99, "reasoning": "test"}',
        role="assistant"
    )

    msg = await provider.chat([{"role": "user", "content": "hello"}])
    assert "PROPOSE_MATCH" in msg.content

def test_provider_selection(monkeypatch):
    monkeypatch.setattr(settings, "AI_PROVIDER", "mock")
    provider = get_ai_provider()
    assert isinstance(provider, MockAIProvider)

    monkeypatch.setattr(settings, "AI_PROVIDER", "openai")
    provider = get_ai_provider()
    assert isinstance(provider, OpenAIProvider)

@pytest.mark.asyncio
async def test_openrouter_missing_key(monkeypatch):
    monkeypatch.setattr(settings, "OPENROUTER_API_KEY", None)
    provider = OpenRouterProvider()
    with pytest.raises(ValueError, match="missing"):
        await provider.chat([])
