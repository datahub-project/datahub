import base64
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List

import requests
from isodate import parse_duration
from requests.adapters import HTTPAdapter
from urllib3 import Retry

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

    @abstractmethod
    def get_authorization_header(self) -> Dict:
        pass

    @abstractmethod
    def get_workspaces_endpoint(self) -> str:
        pass

    @abstractmethod
    def get_workspaces(self) -> List[Workspace]:
        pass

    def get_workspaces_from_response(self, response: List[Dict]) -> List[Workspace]:
        workspaces: List[Workspace] = [
            Workspace(
                workspace_id=workspace.get(Constant.WORKSPACEID),
                name=workspace.get(Constant.NAME),
            )
            for workspace in response
        ]
        return workspaces

    @abstractmethod
    def get_connections_endpoint(self, workspace: Workspace) -> str:
        pass

    @abstractmethod
    def get_connections(self, workspace: Workspace) -> List[Connection]:
        pass

    def get_connections_from_response(self, response: List[Dict]) -> List[Connection]:
        connections: List[Connection] = [
            Connection(
                connection_id=connection.get(Constant.CONNECTIONID),
                name=connection.get(Constant.NAME),
                source_id=connection.get(Constant.SOURCEID),
                destination_id=connection.get(Constant.DESTINATIONID),
                workspace_id=connection.get(Constant.WORKSPACEID),
                status=connection.get(Constant.STATUS),
                namespace_definition=connection.get(Constant.NAMESPACEDEFINITION),
                namespace_format=connection.get(Constant.NAMESPACEFORMAT),
                prefix=connection.get(Constant.PREFIX),
            )
            for connection in response
        ]
        return connections

    @abstractmethod
    def get_source_endpoint(self, source_id: str) -> str:
        pass

    @abstractmethod
    def get_source(self, source_id: str) -> Source:
        pass

    def get_source_from_response(self, response: Dict) -> Source:
        return Source(
            source_id=response.get(Constant.SOURCEID),
            name=response.get(Constant.NAME),
            source_type=response.get(Constant.SOURCETYPE)
            if Constant.SOURCETYPE in response
            else response.get(Constant.SOURCENAME),
            workspace_id=response.get(Constant.WORKSPACEID),
        )

    @abstractmethod
    def get_destination_endpoint(self, destination_id: str) -> str:
        pass

    @abstractmethod
    def get_destination(self, destination_id: str) -> Destination:
        pass

    def get_destination_from_response(self, response: Dict) -> Destination:
        return Destination(
            destination_id=response.get(Constant.DESTINATIONID),
            name=response.get(Constant.NAME),
            destination_type=response.get(Constant.DESTINATIONTYPE)
            if Constant.DESTINATIONTYPE in response
            else response.get(Constant.DESTINATIONNAME),
            workspace_id=response.get(Constant.WORKSPACEID),
        )

    @abstractmethod
    def get_jobs_endpoint(self, connection_id: str) -> str:
        pass

    @abstractmethod
    def get_jobs(self, connection_id: str) -> List[Job]:
        pass

    def get_jobs_from_response(self, response: List[Dict]) -> List[Job]:
        jobs: List[Job] = [
            Job(
                job_id=job.get(Constant.JOBID)
                if Constant.JOBID in job
                else job.get(Constant.ID),
                status=job.get(Constant.STATUS),
                job_type=job.get(Constant.JOBTYPE)
                if Constant.JOBTYPE in job
                else job.get(Constant.CONFIGTYPE),
                start_time=job.get(Constant.STARTTIME)
                if Constant.STARTTIME in job
                else job.get(Constant.CREATEDAT),
                end_time=job.get(Constant.ENDTIME)
                if Constant.ENDTIME in job
                else job.get(Constant.ENDEDAT),
                connection_id=job.get(Constant.CONNECTIONID)
                if Constant.CONNECTIONID in job
                else job.get(Constant.CONFIGID),
                last_updated_at=job.get(Constant.LASTUPDATEDAT)
                if Constant.LASTUPDATEDAT in job
                else job.get(Constant.UPDATEDAT),
                bytes_synced=job.get(Constant.BYTESSYNCED),
                rows_synced=job.get(Constant.ROWSSYNCED)
                if Constant.ROWSSYNCED in job
                else job.get(Constant.RECORDSSYNCED),
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

    def get_authorization_header(self) -> Dict:
        authorization_header = {
            "User-Agent": "DataHub-RestClient",
            "accept": "application/json",
            Constant.AUTHORIZATION: "Bearer {}".format(self.api_key),
        }
        return authorization_header

    def get_workspaces_endpoint(self) -> str:
        workspaces_endpoint: str = CloudAPIResolver.API_ENDPOINTS[
            Constant.WORKSPACE_LIST
        ]
        # Replace place holders
        return workspaces_endpoint.format(BASE_URL=CloudAPIResolver.BASE_URL)

    def get_workspaces(self) -> List[Workspace]:
        """
        Get the list of workspaces from Airbyte Cloud
        """
        workspaces_list_endpoint: str = self.get_workspaces_endpoint()
        logger.debug(f"Request to URL={workspaces_list_endpoint}")
        response = self._request_session.get(
            workspaces_list_endpoint,
            headers=self.get_authorization_header(),
        )
        return self.get_workspaces_from_response(response.json()[Constant.DATA])

    def get_connections_endpoint(self, workspace: Workspace) -> str:
        connections_endpoint: str = CloudAPIResolver.API_ENDPOINTS[
            Constant.CONNECTION_LIST
        ]
        # Replace place holders
        return connections_endpoint.format(
            BASE_URL=CloudAPIResolver.BASE_URL, WORKSPACE_ID=workspace.workspace_id
        )

    def get_connections(self, workspace: Workspace) -> List[Connection]:
        """
        Get the list of connections from Airbyte Cloud
        """
        connections_list_endpoint: str = self.get_connections_endpoint(workspace)
        logger.debug(f"Request to URL={connections_list_endpoint}")
        response = self._request_session.get(
            connections_list_endpoint,
            headers=self.get_authorization_header(),
        )
        return self.get_connections_from_response(response.json()[Constant.DATA])

    def get_source_endpoint(self, source_id: str) -> str:
        source_endpoint: str = CloudAPIResolver.API_ENDPOINTS[Constant.SOURCE_GET]
        # Replace place holders
        return source_endpoint.format(
            BASE_URL=CloudAPIResolver.BASE_URL, SOURCE_ID=source_id
        )

    def get_source(self, source_id: str) -> Source:
        """
        Get the source details from Airbyte Cloud
        """
        source_endpoint: str = self.get_source_endpoint(source_id)
        logger.debug(f"Request to URL={source_endpoint}")
        response = self._request_session.get(
            source_endpoint,
            headers=self.get_authorization_header(),
        )
        return self.get_source_from_response(response.json())

    def get_destination_endpoint(self, destination_id: str) -> str:
        destination_endpoint: str = CloudAPIResolver.API_ENDPOINTS[
            Constant.DESTINATION_GET
        ]
        # Replace place holders
        return destination_endpoint.format(
            BASE_URL=CloudAPIResolver.BASE_URL, DESTINATION_ID=destination_id
        )

    def get_destination(self, destination_id: str) -> Destination:
        """
        Get the destination details from Airbyte Cloud
        """
        destination_endpoint: str = self.get_destination_endpoint(destination_id)
        logger.debug(f"Request to URL={destination_endpoint}")
        response = self._request_session.get(
            destination_endpoint,
            headers=self.get_authorization_header(),
        )
        return self.get_destination_from_response(response.json())

    def get_jobs_endpoint(self, connection_id: str) -> str:
        jobs_endpoint: str = CloudAPIResolver.API_ENDPOINTS[Constant.JOBS_LIST]
        # Replace place holders
        return jobs_endpoint.format(
            BASE_URL=CloudAPIResolver.BASE_URL, CONNECTION_ID=connection_id
        )

    def get_jobs(self, connection_id: str) -> List[Job]:
        """
        Get the jobs list from Airbyte Cloud
        """
        jobs_endpoint: str = self.get_jobs_endpoint(connection_id)
        logger.debug(f"Request to URL={jobs_endpoint}")
        response = self._request_session.get(
            jobs_endpoint,
            headers=self.get_authorization_header(),
        )
        jobs = response.json()[Constant.DATA]
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

        return self.get_jobs_from_response(jobs)


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

    def get_authorization_header(self) -> Dict:
        authorization_header = {
            "accept": "application/json",
            Constant.AUTHORIZATION: "Basic {}".format(
                base64.b64encode(
                    f"{self.username}:{self.password}".encode(Constant.ASCII)
                ).decode(Constant.ASCII)
            ),
        }
        return authorization_header

    def get_workspaces_endpoint(self) -> str:
        workspaces_endpoint: str = OssAPIResolver.API_ENDPOINTS[Constant.WORKSPACE_LIST]
        # Replace place holders
        return workspaces_endpoint.format(BASE_URL=OssAPIResolver.BASE_URL)

    def get_workspaces(self) -> List[Workspace]:
        """
        Get the list of workspaces from Airbyte OSS
        """
        workspaces_list_endpoint: str = self.get_workspaces_endpoint()
        logger.debug(f"Request to URL={workspaces_list_endpoint}")
        response = self._request_session.post(
            workspaces_list_endpoint,
            headers=self.get_authorization_header(),
        )
        return self.get_workspaces_from_response(response.json()[Constant.WORKSPACES])

    def get_connections_endpoint(self, workspace: Workspace) -> str:
        connections_endpoint: str = OssAPIResolver.API_ENDPOINTS[
            Constant.CONNECTION_LIST
        ]
        # Replace place holders
        return connections_endpoint.format(
            BASE_URL=OssAPIResolver.BASE_URL, WORKSPACE_ID=workspace.workspace_id
        )

    def get_connections(self, workspace: Workspace) -> List[Connection]:
        """
        Get the list of connections from Airbyte OSS
        """
        connections_list_endpoint: str = self.get_connections_endpoint(workspace)
        logger.debug(f"Request to URL={connections_list_endpoint}")
        response = self._request_session.post(
            connections_list_endpoint,
            headers=self.get_authorization_header(),
            json={Constant.WORKSPACEID: workspace.workspace_id},
        )
        return self.get_connections_from_response(response.json()[Constant.CONNECTIONS])

    def get_source_endpoint(self, source_id: str) -> str:
        source_endpoint: str = OssAPIResolver.API_ENDPOINTS[Constant.SOURCE_GET]
        # Replace place holders
        return source_endpoint.format(
            BASE_URL=OssAPIResolver.BASE_URL, SOURCE_ID=source_id
        )

    def get_source(self, source_id: str) -> Source:
        """
        Get the source details from Airbyte OSS
        """
        source_endpoint: str = self.get_source_endpoint(source_id)
        logger.debug(f"Request to URL={source_endpoint}")
        response = self._request_session.post(
            source_endpoint,
            headers=self.get_authorization_header(),
            json={Constant.SOURCEID: source_id},
        )
        return self.get_source_from_response(response.json())

    def get_destination_endpoint(self, destination_id: str) -> str:
        destination_endpoint: str = OssAPIResolver.API_ENDPOINTS[
            Constant.DESTINATION_GET
        ]
        # Replace place holders
        return destination_endpoint.format(
            BASE_URL=OssAPIResolver.BASE_URL, DESTINATION_ID=destination_id
        )

    def get_destination(self, destination_id: str) -> Destination:
        """
        Get the destination details from Airbyte OSS
        """
        destination_endpoint: str = self.get_destination_endpoint(destination_id)
        logger.debug(f"Request to URL={destination_endpoint}")
        response = self._request_session.post(
            destination_endpoint,
            headers=self.get_authorization_header(),
            json={Constant.DESTINATIONID: destination_id},
        )
        return self.get_destination_from_response(response.json())

    def get_jobs_endpoint(self, connection_id: str) -> str:
        jobs_endpoint: str = OssAPIResolver.API_ENDPOINTS[Constant.JOBS_LIST]
        # Replace place holders
        return jobs_endpoint.format(
            BASE_URL=OssAPIResolver.BASE_URL, CONNECTION_ID=connection_id
        )

    def get_jobs(self, connection_id: str) -> List[Job]:
        """
        Get the jobs list from Airbyte OSS
        """
        jobs_endpoint: str = self.get_jobs_endpoint(connection_id)
        logger.debug(f"Request to URL={jobs_endpoint}")
        response = self._request_session.post(
            jobs_endpoint,
            headers=self.get_authorization_header(),
            json={
                Constant.CONFIGTYPE: ["sync", "reset_connection"],
                Constant.CONFIGID: connection_id,
            },
        )
        jobs = response.json()[Constant.JOBS]
        for job in jobs:
            last_attempt = job[Constant.ATTEMPTS][-1]
            job.update(job[Constant.JOBS])
            job[Constant.ENDEDAT] = last_attempt[Constant.ENDEDAT]
            job[Constant.RECORDSSYNCED] = last_attempt[Constant.RECORDSSYNCED]
            del job[Constant.ATTEMPTS]
            del job[Constant.JOBS]

        return self.get_jobs_from_response(jobs)


obj = OssAPIResolver(username="airbyte", password="password")
w = obj.get_workspaces()
c = obj.get_connections(w[0])
s = obj.get_source(c[0].source_id)
d = obj.get_destination(c[0].destination_id)
j = obj.get_jobs(c[0].connection_id)

# obj = CloudAPIResolver(api_key="eyJhbGciOiJSUzI1NiIsImtpZCI6IjUxYmQzZDYxLWNhMzMtNGJkYi04ZTE3LWU0Y2IxZGZmOWRmMCIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsiY2xhazFzdTU5MDAwMDNiNmNqNW1tcWc4dSJdLCJjdXN0b21lcl9pZCI6IjdjZjA1NzNmLTA4ZTAtNDk5Yi1hNzUzLTRlMmNlZTVmOWJlNCIsImVtYWlsIjoic2h1YmhhbS5qYWd0YXBAZ3NsYWIuY29tIiwiZW1haWxfdmVyaWZpZWQiOiJ0cnVlIiwiZXhwIjoyNTM0MDIyMTQ0MDAsImlhdCI6MTY4OTg1NzIxNCwiaXNzIjoiaHR0cHM6Ly9hcHAuc3BlYWtlYXN5YXBpLmRldi92MS9hdXRoL29hdXRoL2NsYWsxc3U1OTAwMDAzYjZjajVtbXFnOHUiLCJqdGkiOiI1MWJkM2Q2MS1jYTMzLTRiZGItOGUxNy1lNGNiMWRmZjlkZjAiLCJraWQiOiI1MWJkM2Q2MS1jYTMzLTRiZGItOGUxNy1lNGNiMWRmZjlkZjAiLCJuYmYiOjE2ODk4NTcxNTQsInNwZWFrZWFzeV9jdXN0b21lcl9pZCI6ImNHRkFvSFByYkRlWHVQM2xXZUVieWMwYllMWjIiLCJzcGVha2Vhc3lfd29ya3NwYWNlX2lkIjoiY2xhazFzdTU5MDAwMDNiNmNqNW1tcWc4dSIsInN1YiI6ImNHRkFvSFByYkRlWHVQM2xXZUVieWMwYllMWjIiLCJ1c2VyX2lkIjoiY0dGQW9IUHJiRGVYdVAzbFdlRWJ5YzBiWUxaMiJ9.qPoFAIaBBfuWy3yctpPMZPq-sxD4xNypLtUJKTTSaXSnvuGALWZgRZk2ueTbD4fLqUMjOC-b4zLpMR0q_sdFp7IBJgxHToGfsOR7Bys8gD6QbFQOe9aSNnzWmoz-nWx8TSEvL4OAkohO_8elBTYbFddubXjhP25EITajShUF4QdG8381UxaVi6c7wfVfaBf0ENjOGvYLiqelEP2CDzKayKOzPVGTnNS1piuBUa1P7MU86o4b6jnAgtSp1WYsUSiFAnDWLFiDzIPNfb9o6LIcciA_1IyoJjPo40OlZZY7VYQlJwqGvPWT7_aMj0zr3DGmXtfIpkEDQtmlcLG8ONrHng")
# w = obj.get_workspaces()
# c = obj.get_connections(w[0])
# s = obj.get_source(c[0].source_id)
# d = obj.get_destination(c[0].destination_id)
# j = obj.get_jobs(c[0].connection_id)

print("")
