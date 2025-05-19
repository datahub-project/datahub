import json
from typing import Collection, Mapping, Union


class SdkUsageError(Exception):
    # TODO: Import from datahub.errors
    pass


class SDKUsageErrorWithExamples(SdkUsageError):
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
