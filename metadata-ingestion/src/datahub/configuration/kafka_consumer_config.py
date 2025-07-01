import inspect
import logging
from typing import Any, Dict, Optional

from datahub.ingestion.api.registry import import_path

logger = logging.getLogger(__name__)


class CallableConsumerConfig:
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

        assert isinstance(call_back, str), (
            "oauth_cb must be a string representing python function reference "
            "in the format <python-module>:<function-name>."
        )

        call_back_fn = import_path(call_back)
        self._validate_call_back_fn_signature(call_back_fn)

        # Set the callback
        self._config[CallableConsumerConfig.CALLBACK_ATTRIBUTE] = call_back_fn

    def _validate_call_back_fn_signature(self, call_back_fn: Any) -> None:
        sig = inspect.signature(call_back_fn)

        num_positional_args = len(
            [
                param
                for param in sig.parameters.values()
                if param.kind
                in (
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                )
                and param.default == inspect.Parameter.empty
            ]
        )

        has_variadic_args = any(
            param.kind == inspect.Parameter.VAR_POSITIONAL
            for param in sig.parameters.values()
        )

        assert num_positional_args == 1 or (
            has_variadic_args and num_positional_args <= 1
        ), "oauth_cb function must accept single positional argument."
