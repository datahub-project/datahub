"""
Module for dao layer of multidimension ms ssas.
"""
from typing import Any, Dict, Union

from requests.auth import HTTPBasicAuth
from requests_kerberos import HTTPKerberosAuth
from ssas.api import ISsasAPI, SsasXmlaAPI
from ssas.config import SsasServerHTTPConfig
from ssas.parser import MdXmlaParser

from .domains import XMLAServer


class MultidimensionSsasAPI(ISsasAPI):
    """
    API endpoints to fetch catalogs, cubes, dimension, measures.
    """

    def __init__(self, config: SsasServerHTTPConfig):
        self.__config = config

        self.__auth = self.__get_auth()
        self.__xmla_api = SsasXmlaAPI(config=self.__config, auth=self.__auth)
        self.__xmla_parser = MdXmlaParser()

    def __get_auth(self) -> Union[HTTPBasicAuth, HTTPKerberosAuth]:
        if self.__config.ssas_instance_auth_type == "HTTPBasicAuth":
            return HTTPBasicAuth(
                username=self.__config.username,
                password=self.__config.password,
            )
        if self.__config.ssas_instance_auth_type == "HTTPKerberosAuth":
            return HTTPKerberosAuth()
        raise TypeError(f"Unsupported auth type: {self.__config.ssas_instance_auth_type}")

    def get_server(self) -> XMLAServer:
        """
        Get server metadata.

        :return: structured metadata dataclass.
        """
        xmla_data = self.__xmla_api.get_server_metadata()
        return XMLAServer(**self.__xmla_parser.get_server(xmla_data))

    @property
    def auth_credentials(self) -> HTTPKerberosAuth:
        """
        Get authorization credentials.

        :return: authorization dataclass.
        """
        return self.__auth

    def get_server_info(self) -> Dict[str, Any]:
        """
        Get server information from metadata.

        :return: server metadata as dictionary.
        """
        return self.__xmla_api.get_server_info()
