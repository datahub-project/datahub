"""
Module xmla communicate classes.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Union

import xmltodict
from requests.auth import HTTPBasicAuth
from requests_kerberos import HTTPKerberosAuth

from .config import SsasServerHTTPConfig
from .tools import MsXmlaTemplates
from .xmlaclient import XmlaClient


class ISsasAPI(ABC):
    @abstractmethod
    def get_server(self):
        pass

    @property
    @abstractmethod
    def auth_credentials(self):
        pass


class SsasXmlaAPI:
    """
    Class for parse ssas xmla server response
    """

    def __init__(self, config: SsasServerHTTPConfig, auth: Union[HTTPKerberosAuth, HTTPBasicAuth]):
        self.__config = config
        self.__auth = auth
        self.__client = XmlaClient(config=config, auth=self.__auth)

    def get_server_info(self) -> Dict[str, Any]:
        """
        Extract server metadata info from response
        """

        server_data_xml = xmltodict.parse(self.get_server_metadata())

        return server_data_xml["soap:Envelope"]["soap:Body"]["DiscoverResponse"][
            "return"
        ]["root"]["row"]["xars:METADATA"]["Server"]

    def get_server_metadata(self) -> str:
        """
        Get ssas server metadata
        """

        return str(self.__client.discover(query=MsXmlaTemplates.QUERY_METADATA))
