import logging
from typing import Dict, List, Optional, Union

from datahub.ingestion.auth.env import build_auth_config_from_env
from datahub.ingestion.auth.registry import AuthConfig
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig

logger = logging.getLogger(__name__)


class BaseApi:
    graph: DataHubGraph

    def __init__(
        self,
        datahub_host: Optional[str] = None,
        datahub_token: Optional[str] = None,
        timeout: Optional[int] = None,
        graph: Optional[DataHubGraph] = None,
        datahub_auth: Optional[AuthConfig] = None,
    ):
        if graph:
            self.graph = graph
            return

        assert datahub_host is not None
        source = "static token" if datahub_token else None
        if datahub_token is None and datahub_auth is None:
            # Env-based OAuth (DATAHUB_AUTH_TYPE) for an explicitly passed host.
            # Resolved here because the emitter only does it for the
            # "__from_env__" sentinel, and the Airflow circuit-breaker operators
            # have no other route: they build their config from
            # DatahubRestHook._get_config(), which cannot express an AuthConfig.
            datahub_auth = build_auth_config_from_env()
            source = f"env OAuth ({datahub_auth.type})" if datahub_auth else None
        elif datahub_auth is not None:
            source = f"declarative auth ({datahub_auth.type})"

        # With no credentials here, the emitter falls back to the
        # DATAHUB_SYSTEM_CLIENT_* pair when it is set — an unauthenticated caller
        # silently acquiring system-user privileges is worth one log line.
        logger.info(
            "GraphQL client for %s authenticating with: %s",
            datahub_host,
            source
            or "no explicit credentials (system client if configured, else unauthenticated)",
        )

        self.graph = DataHubGraph(
            DatahubClientConfig(
                server=datahub_host,
                token=datahub_token,
                auth=datahub_auth,
                timeout_sec=timeout,
            )
        )

    def gen_filter(
        self, filters: Dict[str, Optional[str]]
    ) -> Optional[Dict[str, List[Dict[str, Union[str, List[str]]]]]]:
        filter_expression: Optional[
            Dict[str, List[Dict[str, Union[str, List[str]]]]]
        ] = None
        if not filters:
            return None

        filter_list: List[Dict[str, Union[str, List[str]]]] = []
        for key, value in filters.items():
            if value is None:
                continue
            filter_list.append({"field": key, "values": [value]})

        filter_expression = {"and": filter_list}
        return filter_expression
