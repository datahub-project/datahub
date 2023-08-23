import base64
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List

import requests
from isodate import parse_duration
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.source.airbyte.config import Constant
from datahub.ingestion.source.airbyte.rest_api_wrapper.data_classes import (
    Connection,
    Connector,
    Job,
    Workspace,
)

# Logger instance
logger = logging.getLogger(__name__)


class DataResolverBase(ABC):

    CONNECTOR_SERVER_KEY_MAPPING = {
        "postgres": "host",
    }

    def __init__(
        self,
    ):
        self._request_session = requests.Session()
        # set re-try parameter for request_session
        self._request_session.mount(
            "https://",
            HTTPAdapter(
                max_retries=Retry(
                    total=3,
                    backoff_factor=1,
                    allowed_methods=None,
                    status_forcelist=[429, 500, 502, 503, 504],
                )
            ),
        )

    @property
    @abstractmethod
    def api_url(self) -> str:
        pass

    @property
    @abstractmethod
    def API_ENDPOINTS(self) -> Dict:
        pass

    @abstractmethod
    def _get_authorization_header(self) -> Dict:
        pass

    @abstractmethod
    def test_connection(self):
        pass

    @abstractmethod
    def get_workspaces(self) -> List[Workspace]:
        pass

    @abstractmethod
    def _get_connections(self, workspace_id: str) -> List[Connection]:
        pass

    @abstractmethod
    def _get_source_connector_from_response(self, response: Dict) -> Connector:
        pass

    @abstractmethod
    def _get_source_connector(self, source_id: str) -> Connector:
        pass

    @abstractmethod
    def _get_destination_connector_from_response(self, response: Dict) -> Connector:
        pass

    @abstractmethod
    def _get_destination_connector(self, destination_id: str) -> Connector:
        pass

    @abstractmethod
    def _get_jobs_from_response(self, response: List[Dict]) -> List[Job]:
        pass

    @abstractmethod
    def _get_jobs(self, connection_id: str) -> List[Job]:
        pass

    def _get_workspaces_endpoint(self) -> str:
        workspaces_endpoint: str = self.API_ENDPOINTS[Constant.WORKSPACE_LIST]
        # Replace place holders
        return workspaces_endpoint.format(API_URL=self.api_url)

    def _get_workspaces_from_response(self, response: List[Dict]) -> List[Workspace]:
        workspaces: List[Workspace] = [
            Workspace(
                workspace_id=response_dict[Constant.WORKSPACEID],
                name=response_dict[Constant.NAME],
                connections=self._get_connections(response_dict[Constant.WORKSPACEID]),
            )
            for response_dict in response
        ]
        return workspaces

    def _get_connections_endpoint(self, workspace_id: str) -> str:
        connections_endpoint: str = self.API_ENDPOINTS[Constant.CONNECTION_LIST]
        # Replace place holders
        return connections_endpoint.format(
            API_URL=self.api_url, WORKSPACE_ID=workspace_id
        )

    def _get_connections_from_response(self, response: List[Dict]) -> List[Connection]:
        connections: List[Connection] = [
            Connection(
                connection_id=response_dict[Constant.CONNECTIONID],
                name=response_dict[Constant.NAME],
                source=self._get_source_connector(response_dict[Constant.SOURCEID]),
                destination=self._get_destination_connector(
                    response_dict[Constant.DESTINATIONID]
                ),
                status=response_dict[Constant.STATUS],
                namespace_definition=response_dict[Constant.NAMESPACEDEFINITION],
                namespace_format=response_dict[Constant.NAMESPACEFORMAT],
                prefix=response_dict[Constant.PREFIX],
                jobs=self._get_jobs(response_dict[Constant.CONNECTIONID]),
            )
            for response_dict in response
        ]
        return connections

    def _get_server_from_configuration(
        self, configuration: Dict, connector_type: str
    ) -> str:
        return configuration[self.CONNECTOR_SERVER_KEY_MAPPING[connector_type]]

    def _get_source_endpoint(self, source_id: str) -> str:
        source_endpoint: str = self.API_ENDPOINTS[Constant.SOURCE_GET]
        # Replace place holders
        return source_endpoint.format(API_URL=self.api_url, SOURCE_ID=source_id)

    def _get_destination_endpoint(self, destination_id: str) -> str:
        destination_endpoint: str = self.API_ENDPOINTS[Constant.DESTINATION_GET]
        # Replace place holders
        return destination_endpoint.format(
            API_URL=self.api_url, DESTINATION_ID=destination_id
        )

    def _get_jobs_endpoint(self, connection_id: str) -> str:
        jobs_endpoint: str = self.API_ENDPOINTS[Constant.JOBS_LIST]
        # Replace place holders
        return jobs_endpoint.format(API_URL=self.api_url, CONNECTION_ID=connection_id)


class CloudAPIResolver(DataResolverBase):
    api_url = ""
    # Cloud api endpoints
    API_ENDPOINTS = {
        Constant.WORKSPACE_LIST: "{API_URL}/v1/workspaces",
        Constant.CONNECTION_LIST: "{API_URL}/v1/connections?workspaceIds={WORKSPACE_ID}",
        Constant.SOURCE_GET: "{API_URL}/v1/sources/{SOURCE_ID}",
        Constant.DESTINATION_GET: "{API_URL}/v1/destinations/{DESTINATION_ID}",
        Constant.JOBS_LIST: "{API_URL}/v1/jobs?connectionId={CONNECTION_ID}",
    }

    def __init__(
        self,
        api_url,
        api_key,
    ):
        super().__init__()
        self.api_url = api_url
        self.api_key = api_key

    def _get_authorization_header(self) -> Dict:
        authorization_header = {
            "User-Agent": "DataHub-RestClient",
            "accept": "application/json",
            Constant.AUTHORIZATION: "Bearer {}".format(self.api_key),
        }
        return authorization_header

    def test_connection(self):
        logger.debug(f"Testing connection to api = {self.api_url}")
        # testing api connection by fetching workspaces
        response = self._request_session.get(
            self._get_workspaces_endpoint(),
            headers=self._get_authorization_header(),
        )
        if response.status_code == 401:
            raise ConfigurationError(
                "Please check if provided api key is correct or not."
            )
        response.raise_for_status()

    def get_workspaces(self) -> List[Workspace]:
        """
        Get the list of workspaces from Airbyte Cloud
        """
        workspaces_list_endpoint: str = self._get_workspaces_endpoint()
        logger.debug(f"Request to URL={workspaces_list_endpoint}")
        response = self._request_session.get(
            workspaces_list_endpoint,
            headers=self._get_authorization_header(),
        )
        response.raise_for_status()
        workspaces = response.json().get(Constant.DATA)
        return self._get_workspaces_from_response(workspaces) if workspaces else []

    def _get_connections(self, workspace_id: str) -> List[Connection]:
        """
        Get the list of connections from Airbyte Cloud
        """
        connections_list_endpoint: str = self._get_connections_endpoint(workspace_id)
        logger.debug(f"Request to URL={connections_list_endpoint}")
        response = self._request_session.get(
            connections_list_endpoint,
            headers=self._get_authorization_header(),
        )
        response.raise_for_status()
        connections = response.json().get(Constant.DATA)
        return self._get_connections_from_response(connections) if connections else []

    def _get_source_connector_from_response(self, response: Dict) -> Connector:
        connector_type = response[Constant.SOURCETYPE].lower()
        configuration = response[Constant.CONFIGURATION]
        return Connector(
            connector_id=response[Constant.SOURCEID],
            name=response[Constant.NAME],
            type=connector_type,
            server=self._get_server_from_configuration(configuration, connector_type),
        )

    def _get_source_connector(self, source_id: str) -> Connector:
        """
        Get the source connector details from Airbyte Cloud
        """
        source_endpoint: str = self._get_source_endpoint(source_id)
        logger.debug(f"Request to URL={source_endpoint}")
        response = self._request_session.get(
            source_endpoint,
            headers=self._get_authorization_header(),
        )
        response.raise_for_status()
        return self._get_source_connector_from_response(response.json())

    def _get_destination_connector_from_response(self, response: Dict) -> Connector:
        connector_type = response[Constant.DESTINATIONTYPE].lower()
        configuration = response[Constant.CONFIGURATION]
        return Connector(
            connector_id=response[Constant.DESTINATIONID],
            name=response[Constant.NAME],
            type=connector_type,
            server=self._get_server_from_configuration(configuration, connector_type),
        )

    def _get_destination_connector(self, destination_id: str) -> Connector:
        """
        Get the destination connector details from Airbyte Cloud
        """
        destination_endpoint: str = self._get_destination_endpoint(destination_id)
        logger.debug(f"Request to URL={destination_endpoint}")
        response = self._request_session.get(
            destination_endpoint,
            headers=self._get_authorization_header(),
        )
        response.raise_for_status()
        return self._get_destination_connector_from_response(response.json())

    def _get_jobs_from_response(self, response: List[Dict]) -> List[Job]:
        jobs: List[Job] = [
            Job(
                job_id=str(job[Constant.JOBID]),
                status=job[Constant.STATUS],
                job_type=job[Constant.JOBTYPE],
                start_time=job[Constant.STARTTIME],
                end_time=job[Constant.ENDTIME],
                last_updated_at=job[Constant.LASTUPDATEDAT],
                bytes_synced=job[Constant.BYTESSYNCED],
                rows_synced=job[Constant.ROWSSYNCED],
            )
            for job in response
        ]
        return jobs

    def _get_jobs(self, connection_id: str) -> List[Job]:
        """
        Get the jobs list from Airbyte Cloud
        """
        jobs_endpoint: str = self._get_jobs_endpoint(connection_id)
        logger.debug(f"Request to URL={jobs_endpoint}")
        response = self._request_session.get(
            jobs_endpoint,
            headers=self._get_authorization_header(),
        )
        response.raise_for_status()
        jobs = response.json().get(Constant.DATA)
        if jobs:
            for job in jobs:
                # Convert string to timestamp
                start_time = datetime.strptime(
                    job[Constant.STARTTIME], "%Y-%m-%dT%H:%M:%SZ"
                )
                job[Constant.STARTTIME] = round(start_time.timestamp())
                # Convert ISO_8601 format duration to timedelta
                # And add it with start time to get ent time. Ex: 'PT1M10S' to
                end_time = start_time + parse_duration(job[Constant.DURATION])
                job[Constant.ENDTIME] = round(end_time.timestamp())
            return self._get_jobs_from_response(jobs)
        else:
            return []


class OssAPIResolver(DataResolverBase):
    api_url = ""
    # Oss api endpoints
    API_ENDPOINTS = {
        Constant.WORKSPACE_LIST: "{API_URL}/v1/workspaces/list",
        Constant.CONNECTION_LIST: "{API_URL}/v1/connections/list",
        Constant.SOURCE_GET: "{API_URL}/v1/sources/get",
        Constant.DESTINATION_GET: "{API_URL}/v1/destinations/get",
        Constant.JOBS_LIST: "{API_URL}/v1/jobs/list",
    }

    def __init__(
        self,
        api_url,
        username,
        password,
    ):
        super().__init__()
        self.api_url = api_url
        self.username = username
        self.password = password

    def _get_authorization_header(self) -> Dict:
        authorization_header = {
            "accept": "application/json",
            Constant.AUTHORIZATION: "Basic {}".format(
                base64.b64encode(
                    f"{self.username}:{self.password.get_secret_value()}".encode(
                        Constant.ASCII
                    )
                ).decode(Constant.ASCII)
            ),
        }
        return authorization_header

    def test_connection(self):
        logger.debug(f"Testing connection to api = {self.api_url}")
        # testing api connection by fetching workspaces
        response = self._request_session.post(
            self._get_workspaces_endpoint(),
            headers=self._get_authorization_header(),
        )
        if response.status_code == 401:
            raise ConfigurationError(
                "Please check if provided username and password is correct or not."
            )
        response.raise_for_status()

    def get_workspaces(self) -> List[Workspace]:
        """
        Get the list of workspaces from Airbyte OSS
        """
        workspaces_list_endpoint: str = self._get_workspaces_endpoint()
        logger.debug(f"Request to URL={workspaces_list_endpoint}")
        response = self._request_session.post(
            workspaces_list_endpoint,
            headers=self._get_authorization_header(),
        )
        response.raise_for_status()
        return self._get_workspaces_from_response(response.json()[Constant.WORKSPACES])

    def _get_connections(self, workspace_id: str) -> List[Connection]:
        """
        Get the list of connections from Airbyte OSS
        """
        connections_list_endpoint: str = self._get_connections_endpoint(workspace_id)
        logger.debug(f"Request to URL={connections_list_endpoint}")
        response = self._request_session.post(
            connections_list_endpoint,
            headers=self._get_authorization_header(),
            json={Constant.WORKSPACEID: workspace_id},
        )
        response.raise_for_status()
        return self._get_connections_from_response(
            response.json()[Constant.CONNECTIONS]
        )

    def _get_source_connector_from_response(self, response: Dict) -> Connector:
        connector_type = response[Constant.SOURCENAME].lower()
        configuration = response[Constant.CONNECTIONCONFIGURATION]
        return Connector(
            connector_id=response[Constant.SOURCEID],
            name=response[Constant.NAME],
            type=connector_type,
            server=self._get_server_from_configuration(configuration, connector_type),
        )

    def _get_source_connector(self, source_id: str) -> Connector:
        """
        Get the source connector details from Airbyte OSS
        """
        source_endpoint: str = self._get_source_endpoint(source_id)
        logger.debug(f"Request to URL={source_endpoint}")
        response = self._request_session.post(
            source_endpoint,
            headers=self._get_authorization_header(),
            json={Constant.SOURCEID: source_id},
        )
        response.raise_for_status()
        return self._get_source_connector_from_response(response.json())

    def _get_destination_connector_from_response(self, response: Dict) -> Connector:
        connector_type = response[Constant.DESTINATIONNAME].lower()
        configuration = response[Constant.CONNECTIONCONFIGURATION]
        return Connector(
            connector_id=response[Constant.DESTINATIONID],
            name=response[Constant.NAME],
            type=connector_type,
            server=self._get_server_from_configuration(configuration, connector_type),
        )

    def _get_destination_connector(self, destination_id: str) -> Connector:
        """
        Get the destination connector details from Airbyte OSS
        """
        destination_endpoint: str = self._get_destination_endpoint(destination_id)
        logger.debug(f"Request to URL={destination_endpoint}")
        response = self._request_session.post(
            destination_endpoint,
            headers=self._get_authorization_header(),
            json={Constant.DESTINATIONID: destination_id},
        )
        response.raise_for_status()
        return self._get_destination_connector_from_response(response.json())

    def _get_jobs_from_response(self, response: List[Dict]) -> List[Job]:
        jobs: List[Job] = [
            Job(
                job_id=str(job[Constant.ID]),
                status=job[Constant.STATUS],
                job_type=job[Constant.CONFIGTYPE],
                start_time=job[Constant.CREATEDAT],
                end_time=job[Constant.ENDEDAT],
                last_updated_at=job[Constant.UPDATEDAT],
                bytes_synced=job[Constant.BYTESSYNCED],
                rows_synced=job[Constant.RECORDSSYNCED],
            )
            for job in response
        ]
        return jobs

    def _get_jobs(self, connection_id: str) -> List[Job]:
        """
        Get the jobs list from Airbyte OSS
        """
        jobs_endpoint: str = self._get_jobs_endpoint(connection_id)
        logger.debug(f"Request to URL={jobs_endpoint}")
        response = self._request_session.post(
            jobs_endpoint,
            headers=self._get_authorization_header(),
            json={
                Constant.CONFIGTYPES: ["sync", "reset_connection"],
                Constant.CONFIGID: connection_id,
            },
        )
        response.raise_for_status()
        jobs = response.json()[Constant.JOBS]
        for job in jobs:
            last_attempt = job[Constant.ATTEMPTS][-1]
            job.update(job[Constant.JOB])
            job[Constant.ENDEDAT] = last_attempt[Constant.ENDEDAT]
            job[Constant.BYTESSYNCED] = last_attempt[Constant.BYTESSYNCED]
            job[Constant.RECORDSSYNCED] = last_attempt[Constant.RECORDSSYNCED]
            del job[Constant.ATTEMPTS]
            del job[Constant.JOB]

        return self._get_jobs_from_response(jobs)
