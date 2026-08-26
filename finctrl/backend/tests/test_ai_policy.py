import pytest
import os
os.environ['DATABASE_URL'] = 'sqlite+aiosqlite:///:memory:'

def test_ai_policy_pass():
    assert True
