import importlib
import logging
from typing import Any, Dict, Optional

from confluent_kafka import Consumer

logger = logging.getLogger(__name__)


class CallableConsumerConfig(Consumer):
    CALLBACK_ATTRIBUTE: str = "oauth_cb"

    def __init__(self, config: Dict[str, Any]):
        self._config = config

        self._resolve_oauth_callback()

    def callable_config(self) -> Dict[str, Any]:
        return self._config

    @staticmethod
    def is_callable_config(config: Dict[str, Any]) -> bool:
        return CallableConsumerConfig.CALLBACK_ATTRIBUTE in config

    def get_call_back_attribute(self) -> Optional[str]:
        return self._config.get(CallableConsumerConfig.CALLBACK_ATTRIBUTE)

    def _resolve_oauth_callback(self) -> None:
        if not self.get_call_back_attribute():
            return

        call_back = self.get_call_back_attribute()

        assert call_back  # to silent lint

        parts = call_back.split(":")

        module, function = (parts + [None])[:2]  # Ensure at least two elements

        if module is None or function is None:
            # TODO: Refactor the code of kafka to pass reporter here
            logger.warning(
                f"The `{CallableConsumerConfig.CALLBACK_ATTR}` must follow the format `module:function`."
            )
            return

        module_instance = importlib.import_module(module)

        if not hasattr(module_instance, function):
            # TODO: Refactor the code of kafka to pass reporter here
            logger.warning(
                f"The function `{function}` is not found in the module `{module}`."
            )
            return

        # Set the actual function call
        self._config[CallableConsumerConfig.CALLBACK_ATTRIBUTE] = getattr(
            module_instance, function
        )
