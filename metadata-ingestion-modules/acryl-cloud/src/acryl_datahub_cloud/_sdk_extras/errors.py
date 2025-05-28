import json
from typing import Collection, Mapping, Union


class SDKUsageError(Exception):
    def __init__(self, msg: str):
        super().__init__(msg)


class SDKUsageErrorWithExamples(SDKUsageError):
    def __init__(
        self,
        msg: str,
        examples: Union[
            Mapping[str, Union[dict[str, str], str]], dict[str, Collection[str]]
        ],
    ):
        super().__init__(msg)
        self.msg = msg
        self.examples = examples

    def __str__(self) -> str:
        examples_str = "\n\n".join(
            f"Example - {key}:\n{json.dumps(value, indent=2) if isinstance(value, dict) else value}"
            for key, value in self.examples.items()
        )
        return f"{self.msg}\n\n*** Examples of Accepted Values ***\n\n{examples_str}\n\n*** End of Examples ***"


class SDKNotYetSupportedError(Exception):
    def __init__(self, supported_item: str):
        msg = f"This feature is not yet supported in the Python SDK: {supported_item}"
        super().__init__(msg)
        self.msg = msg
