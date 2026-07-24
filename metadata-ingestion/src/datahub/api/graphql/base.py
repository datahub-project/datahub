from typing import Dict, List, Optional, Union

from gql import Client
from gql.transport.requests import RequestsHTTPTransport

from datahub.api.gql_transport import build_gql_transport
from datahub.ingestion.auth.registry import AuthConfig


class BaseApi:
    client: Client

    def __init__(
        self,
        datahub_host: Optional[str] = None,
        datahub_token: Optional[str] = None,
        timeout: Optional[int] = None,
        transport: Optional[RequestsHTTPTransport] = None,
        datahub_auth: Optional[AuthConfig] = None,
    ):
        if transport:
            self.transport = transport
        else:
            assert datahub_host is not None
            self.transport = build_gql_transport(
                url=datahub_host,
                token=datahub_token,
                auth=datahub_auth,
                timeout=timeout,
            )

        self.client = Client(
            transport=self.transport,
            fetch_schema_from_transport=True,
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
