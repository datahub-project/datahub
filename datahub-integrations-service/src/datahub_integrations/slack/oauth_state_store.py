import logging
import time
from logging import Logger
from typing import Any, Dict
from uuid import uuid4

from slack_sdk.oauth.state_store import OAuthStateStore

logger = logging.getLogger(__name__)


class InMemoryStateStore(OAuthStateStore):
    """A simple in-memory implementation of the Slack's OAuthStateStore interface."""

    # This was heavily inspired by the Slack's file-based implementation
    # in slack_sdk.oauth.state_store.FileOAuthStateStore

    def __init__(
        self,
        *,
        expiration_seconds: int,
        logger: Logger = logger,
    ):
        self.expiration_seconds = expiration_seconds

        self._states: Dict[str, float] = {}

        self._logger = logger

    @property
    def logger(self) -> Logger:
        if self._logger is None:
            self._logger = logging.getLogger(__name__)
        return self._logger

    def issue(self, *args: Any, **kwargs: Any) -> str:
        state = str(uuid4())
        self._states[state] = time.time()
        return state

    def consume(self, state: str) -> bool:
        if state not in self._states:
            message = f"Failed to find any persistent data for state: {state}"
            self.logger.warning(message)
            return False

        created = self._states[state]
        expiration = created + self.expiration_seconds
        still_valid: bool = time.time() < expiration

        del self._states[state]  # consume the state by deleting it
        return still_valid
