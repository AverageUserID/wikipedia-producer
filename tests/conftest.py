import importlib.util
import os
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:49092")


@pytest.fixture
def producer_module(monkeypatch):
    """Import wikipedia-producer.py (hyphenated name) with a test broker and unique topic."""
    monkeypatch.setenv("KAFKA_BROKER", BROKER)
    spec = importlib.util.spec_from_file_location("wikipedia_producer", ROOT / "wikipedia-producer.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def topic_name():
    return f"test-wikipedia-edits-{uuid.uuid4().hex[:8]}"
