from typing import Dict, List, Optional, Union

from datahub.ingestion.auth.env import build_auth_config_from_env
from datahub.ingestion.auth.registry import AuthConfig
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig


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
        if datahub_token is None and datahub_auth is None:
            # Env-based OAuth (DATAHUB_AUTH_TYPE) for an explicitly passed host.
            # TODO: drop once the emitter resolves env auth for every server and
            # not just the "__from_env__" sentinel (PR #18547) — this call then
            # becomes redundant.
            datahub_auth = build_auth_config_from_env()

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
