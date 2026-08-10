from dataclasses import dataclass, field
from typing import Dict, List, Protocol, Sequence, runtime_checkable

from datahub.ingestion.agent.api_gate import check_api_request


@runtime_checkable
class ApiQueryProvider(Protocol):
    """A probe provider that can answer a read request against its own API.

    Implementations must NOT expose get_json as an @probe_method: that would put
    an arbitrary path on `probe run`, reaching the API without passing
    check_api_request. execute_scoped_api is the only supported route.
    """

    # Read endpoints this connector permits, as "GET /path/{placeholder}".
    # A placeholder matches exactly one path segment. Empty means the connector
    # has not opted in, and nothing is reachable.
    api_allowlist: Sequence[str]

    def get_json(self, path: str) -> object:
        """Fetch one already-checked path, relative to the connector's base URI."""
        ...


@dataclass
class ApiQueryResult:
    source_type: str
    method: str
    path: str
    result: object
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        return {
            "source_type": self.source_type,
            "method": self.method,
            "path": self.path,
            "result": self.result,
            "warnings": self.warnings,
        }


def execute_scoped_api(
    provider: object, source_type: str, method: str, path: str
) -> ApiQueryResult:
    """Check the request, then run it against an already-open provider.

    The check runs before the provider is touched, so a refused request never
    reaches the connector's client.
    """
    if not isinstance(provider, ApiQueryProvider):
        raise ValueError(
            f"source '{source_type}' does not expose an API probe surface; "
            f"use `probe methods` to see what it does offer"
        )

    check_api_request(method, path, provider.api_allowlist)
    result = provider.get_json(path)

    provider_warnings = getattr(provider, "warnings", None)
    return ApiQueryResult(
        source_type=source_type,
        method=method.upper(),
        path=path,
        result=result,
        warnings=list(provider_warnings) if provider_warnings else [],
    )


def run_probe_api(
    source_type: str, config_dict: Dict[str, object], method: str, path: str
) -> ApiQueryResult:
    from datahub.ingestion.agent.probe_methods import config_class_for

    config_cls = config_class_for(source_type)
    if config_cls is None:
        raise ValueError(f"source '{source_type}' has no probe configuration")
    config = config_cls.model_validate(config_dict)

    build = getattr(config, "build_probe_provider", None)
    if build is None:
        raise ValueError(f"source '{source_type}' does not expose an API probe surface")

    with build() as provider:
        return execute_scoped_api(provider, source_type, method, path)
