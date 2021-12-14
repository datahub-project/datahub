import urllib.parse
from json.decoder import JSONDecodeError
from typing import Dict, Optional, Type, TypeVar

from avrogen.dict_wrapper import DictWrapper
from requests.models import HTTPError
from requests.sessions import Session

from datahub.configuration.common import ConfigModel, OperationalError
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import OwnershipClass

# This bound isn't tight, but it's better than nothing.
Aspect = TypeVar("Aspect", bound=DictWrapper)


class DatahubClientConfig(ConfigModel):
    """Configuration class for holding connectivity to datahub gms"""

    server: str = "http://localhost:8080"
    token: Optional[str]
    timeout_sec: Optional[int]
    extra_headers: Optional[Dict[str, str]]
    ca_certificate_path: Optional[str]
    max_threads: int = 1


class DataHubGraph(DatahubRestEmitter):
    def __init__(self, config: DatahubClientConfig) -> None:
        self.config = config
        super().__init__(
            gms_server=self.config.server,
            token=self.config.token,
            connect_timeout_sec=self.config.timeout_sec,  # reuse timeout_sec for connect timeout
            read_timeout_sec=self.config.timeout_sec,
            extra_headers=self.config.extra_headers,
            ca_certificate_path=self.config.ca_certificate_path,
        )
        self.test_connection()
        self.g_session = Session()

    def _get_generic(self, url: str) -> Dict:
        try:
            response = self.g_session.get(url)
            response.raise_for_status()
            return response.json()
        except HTTPError as e:
            try:
                info = response.json()
                raise OperationalError(
                    "Unable to get metadata from DataHub", info
                ) from e
            except JSONDecodeError:
                # If we can't parse the JSON, just raise the original error.
                raise OperationalError(
                    "Unable to get metadata from DataHub", {"message": str(e)}
                ) from e

    def get_aspect(
        self,
        entity_urn: str,
        aspect: str,
        aspect_type_name: str,
        aspect_type: Type[Aspect],
    ) -> Optional[Aspect]:
        url = f"{self._gms_server}/aspects/{urllib.parse.quote(entity_urn)}?aspect={aspect}&version=0"
        response = self.g_session.get(url)
        if response.status_code == 404:
            # not found
            return None
        response.raise_for_status()
        response_json = response.json()
        aspect_json = response_json.get("aspect", {}).get(aspect_type_name)
        if aspect_json:
            return aspect_type.from_obj(aspect_json, tuples=True)
        else:
            raise OperationalError(
                f"Failed to find {aspect_type_name} in response {response_json}"
            )

    def get_ownership(self, entity_urn: str) -> Optional[OwnershipClass]:
        return self.get_aspect(
            entity_urn=entity_urn,
            aspect="ownership",
            aspect_type_name="com.linkedin.common.Ownership",
            aspect_type=OwnershipClass,
        )
