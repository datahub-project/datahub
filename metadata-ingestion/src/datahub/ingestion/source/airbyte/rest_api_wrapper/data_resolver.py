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
    Destination,
    Job,
    Source,
    Workspace,
)

# Logger instance
logger = logging.getLogger(__name__)


class DataResolverBase(ABC):
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
    def BASE_URL(self) -> str:
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
    def _get_source(self, source_id: str) -> Source:
        pass

    @abstractmethod
    def _get_destination(self, destination_id: str) -> Destination:
        pass

    @abstractmethod
    def _get_jobs(self, connection_id: str) -> List[Job]:
        pass

    def _get_workspaces_endpoint(self) -> str:
        workspaces_endpoint: str = self.API_ENDPOINTS[Constant.WORKSPACE_LIST]
        # Replace place holders
        return workspaces_endpoint.format(BASE_URL=self.BASE_URL)

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
            BASE_URL=self.BASE_URL, WORKSPACE_ID=workspace_id
        )

    def _get_connections_from_response(self, response: List[Dict]) -> List[Connection]:
        connections: List[Connection] = [
            Connection(
                connection_id=response_dict[Constant.CONNECTIONID],
                name=response_dict[Constant.NAME],
                source=self._get_source(response_dict[Constant.SOURCEID]),
                destination=self._get_destination(
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

    def _get_source_endpoint(self, source_id: str) -> str:
        source_endpoint: str = self.API_ENDPOINTS[Constant.SOURCE_GET]
        # Replace place holders
        return source_endpoint.format(BASE_URL=self.BASE_URL, SOURCE_ID=source_id)

    def _get_source_from_response(self, response: Dict) -> Source:
        return Source(
            source_id=response[Constant.SOURCEID],
            name=response[Constant.NAME],
            source_type=response[Constant.SOURCETYPE]
            if Constant.SOURCETYPE in response
            else response[Constant.SOURCENAME],
        )

    def _get_destination_endpoint(self, destination_id: str) -> str:
        destination_endpoint: str = self.API_ENDPOINTS[Constant.DESTINATION_GET]
        # Replace place holders
        return destination_endpoint.format(
            BASE_URL=self.BASE_URL, DESTINATION_ID=destination_id
        )

    def _get_destination_from_response(self, response: Dict) -> Destination:
        return Destination(
            destination_id=response[Constant.DESTINATIONID],
            name=response[Constant.NAME],
            destination_type=response[Constant.DESTINATIONTYPE]
            if Constant.DESTINATIONTYPE in response
            else response[Constant.DESTINATIONNAME],
        )

    def _get_jobs_endpoint(self, connection_id: str) -> str:
        jobs_endpoint: str = self.API_ENDPOINTS[Constant.JOBS_LIST]
        # Replace place holders
        return jobs_endpoint.format(BASE_URL=self.BASE_URL, CONNECTION_ID=connection_id)

    def _get_jobs_from_response(self, response: List[Dict]) -> List[Job]:
        jobs: List[Job] = [
            Job(
                job_id=job[Constant.JOBID]
                if Constant.JOBID in job
                else job[Constant.ID],
                status=job[Constant.STATUS],
                job_type=job[Constant.JOBTYPE]
                if Constant.JOBTYPE in job
                else job[Constant.CONFIGTYPE],
                start_time=job[Constant.STARTTIME]
                if Constant.STARTTIME in job
                else job[Constant.CREATEDAT],
                end_time=job[Constant.ENDTIME]
                if Constant.ENDTIME in job
                else job[Constant.ENDEDAT],
                last_updated_at=job[Constant.LASTUPDATEDAT]
                if Constant.LASTUPDATEDAT in job
                else job[Constant.UPDATEDAT],
                bytes_synced=job[Constant.BYTESSYNCED],
                rows_synced=job[Constant.ROWSSYNCED]
                if Constant.ROWSSYNCED in job
                else job[Constant.RECORDSSYNCED],
            )
            for job in response
        ]
        return jobs


class CloudAPIResolver(DataResolverBase):
    BASE_URL = "https://api.airbyte.com/v1"
    # Cloud api endpoints
    API_ENDPOINTS = {
        Constant.WORKSPACE_LIST: "{BASE_URL}/workspaces",
        Constant.CONNECTION_LIST: "{BASE_URL}/connections?workspaceIds={WORKSPACE_ID}",
        Constant.SOURCE_GET: "{BASE_URL}/sources/{SOURCE_ID}",
        Constant.DESTINATION_GET: "{BASE_URL}/destinations/{DESTINATION_ID}",
        Constant.JOBS_LIST: "{BASE_URL}/jobs?connectionId={CONNECTION_ID}",
    }

    def __init__(
        self,
        api_key,
    ):
        super().__init__()
        self.api_key = api_key

    def _get_authorization_header(self) -> Dict:
        authorization_header = {
            "User-Agent": "DataHub-RestClient",
            "accept": "application/json",
            Constant.AUTHORIZATION: "Bearer {}".format(self.api_key),
        }
        return authorization_header

    def test_connection(self):
        logger.debug(f"Testing connection to api = {self.BASE_URL}")
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

    def _get_source(self, source_id: str) -> Source:
        """
        Get the source details from Airbyte Cloud
        """
        source_endpoint: str = self._get_source_endpoint(source_id)
        logger.debug(f"Request to URL={source_endpoint}")
        response = self._request_session.get(
            source_endpoint,
            headers=self._get_authorization_header(),
        )
        response.raise_for_status()
        return self._get_source_from_response(response.json())

    def _get_destination(self, destination_id: str) -> Destination:
        """
        Get the destination details from Airbyte Cloud
        """
        destination_endpoint: str = self._get_destination_endpoint(destination_id)
        logger.debug(f"Request to URL={destination_endpoint}")
        response = self._request_session.get(
            destination_endpoint,
            headers=self._get_authorization_header(),
        )
        response.raise_for_status()
        return self._get_destination_from_response(response.json())

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
    BASE_URL = "http://localhost:8000/api/v1"
    # Oss api endpoints
    API_ENDPOINTS = {
        Constant.WORKSPACE_LIST: "{BASE_URL}/workspaces/list",
        Constant.CONNECTION_LIST: "{BASE_URL}/connections/list",
        Constant.SOURCE_GET: "{BASE_URL}/sources/get",
        Constant.DESTINATION_GET: "{BASE_URL}/destinations/get",
        Constant.JOBS_LIST: "{BASE_URL}/jobs/list",
    }

    def __init__(
        self,
        username,
        password,
    ):
        super().__init__()
        self.username = username
        self.password = password

    def _get_authorization_header(self) -> Dict:
        authorization_header = {
            "accept": "application/json",
            Constant.AUTHORIZATION: "Basic {}".format(
                base64.b64encode(
                    f"{self.username}:{self.password}".encode(Constant.ASCII)
                ).decode(Constant.ASCII)
            ),
        }
        return authorization_header

    def test_connection(self):
        logger.debug(f"Testing connection to api = {self.BASE_URL}")
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

    def _get_source(self, source_id: str) -> Source:
        """
        Get the source details from Airbyte OSS
        """
        source_endpoint: str = self._get_source_endpoint(source_id)
        logger.debug(f"Request to URL={source_endpoint}")
        response = self._request_session.post(
            source_endpoint,
            headers=self._get_authorization_header(),
            json={Constant.SOURCEID: source_id},
        )
        response.raise_for_status()
        return self._get_source_from_response(response.json())

    def _get_destination(self, destination_id: str) -> Destination:
        """
        Get the destination details from Airbyte OSS
        """
        destination_endpoint: str = self._get_destination_endpoint(destination_id)
        logger.debug(f"Request to URL={destination_endpoint}")
        response = self._request_session.post(
            destination_endpoint,
            headers=self._get_authorization_header(),
            json={Constant.DESTINATIONID: destination_id},
        )
        response.raise_for_status()
        return self._get_destination_from_response(response.json())

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
            job[Constant.RECORDSSYNCED] = last_attempt[Constant.RECORDSSYNCED]
            del job[Constant.ATTEMPTS]
            del job[Constant.JOB]

        return self._get_jobs_from_response(jobs)
