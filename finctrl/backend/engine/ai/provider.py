from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any, Union
import json

from openai import AsyncOpenAI
from openai.types.chat.chat_completion_message import ChatCompletionMessage
from finctrl.backend.engine.ai.schemas import ProposedMatchSchema
from finctrl.backend.config import settings

class AIProvider(ABC):
    @abstractmethod
    async def chat(self, messages: List[Dict[str, Any]], tools: Optional[List[Dict[str, Any]]] = None) -> ChatCompletionMessage:
        pass

class OpenRouterProvider(AIProvider):
    def __init__(self):
        self.api_key = settings.OPENROUTER_API_KEY
        self.model = settings.OPENROUTER_MODEL
        if self.api_key:
            self.client = AsyncOpenAI(
                base_url="https://openrouter.ai/api/v1",
                api_key=self.api_key,
            )
        else:
            self.client = None

    async def chat(self, messages: List[Dict[str, Any]], tools: Optional[List[Dict[str, Any]]] = None) -> ChatCompletionMessage:
        if not self.client:
            raise ValueError("OpenRouter API Key is missing")

        kwargs = {
            "model": self.model,
            "messages": messages,
            "response_format": {"type": "json_object"}
        }
        if tools:
            kwargs["tools"] = tools
            kwargs["tool_choice"] = "auto"
            kwargs["parallel_tool_calls"] = False

        response = await self.client.chat.completions.create(**kwargs)
        return response.choices[0].message

class OpenAIProvider(AIProvider):
    def __init__(self):
        self.api_key = settings.OPENAI_API_KEY
        self.model = "gpt-4o-mini"
        if self.api_key:
            self.client = AsyncOpenAI(api_key=self.api_key)
        else:
            self.client = None

    async def chat(self, messages: List[Dict[str, Any]], tools: Optional[List[Dict[str, Any]]] = None) -> ChatCompletionMessage:
        if not self.client:
            raise ValueError("OpenAI API Key is missing")

        kwargs = {
            "model": self.model,
            "messages": messages,
            "response_format": {"type": "json_object"}
        }
        if tools:
            kwargs["tools"] = tools
            kwargs["tool_choice"] = "auto"
            kwargs["parallel_tool_calls"] = False

        response = await self.client.chat.completions.create(**kwargs)
        return response.choices[0].message

class MockAIProvider(AIProvider):
    def __init__(self):
        self.next_message: Optional[ChatCompletionMessage] = None

    async def chat(self, messages: List[Dict[str, Any]], tools: Optional[List[Dict[str, Any]]] = None) -> ChatCompletionMessage:
        if not self.next_message:
            raise ValueError("Mock Provider next_message is not set")
        return self.next_message

def get_ai_provider() -> AIProvider:
    if settings.AI_PROVIDER == "openrouter":
        return OpenRouterProvider()
    elif settings.AI_PROVIDER == "openai":
        return OpenAIProvider()
    else:
        return MockAIProvider()
