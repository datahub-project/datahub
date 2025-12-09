"""
Abstract base class for chat models used in this project.

Provides a nominal type so concrete chat implementations explicitly implement
the expected interface.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List

from message import Message


class BaseChatModel(ABC):
    """Chat model interface implemented by concrete providers."""

    @abstractmethod
    def invoke(self, messages: List[Any], **kwargs: Any) -> Message:
        """Send chat messages and return a normalized Message."""
        raise NotImplementedError


