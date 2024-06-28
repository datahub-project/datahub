"""
Module with XMLA client.
"""

import logging
from typing import Any, Dict, List

import re
import requests
import xmltodict
from .xmla_server_response_error import XMLAServerResponseError

from .tools import MsXmlaTemplates

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class XmlaResponse:
    """
    Class for parse XMLA ssas server response
    """

    def __init__(self, response: str):
        self.item = response

    def as_dict(self) -> Dict[str, Any]:
        """
        Represent xml as dict

        :return: xml data as dict.
        """
        return xmltodict.parse(self.item)

    def __repr__(self):
        return self.item

    def get_node(self, node_name="row"):
        """
        Get custom node from xml tree
        :return: node as dict
        """

        return self.find_node(tree=self.as_dict(), search_term=node_name)

    def find_node(self, tree: Dict[str, Any], search_term: str) -> List[Any]:
        """
        Find custom node in xml tree

        :return: keys as list
        """
        keys = [*tree.keys()]
        try:
            while len(keys) > 0:
                key = keys.pop(0)
                # search term has been found
                if key == search_term:
                    result = tree[key]
                    if isinstance(result, list):
                        return tree[key]
                    return [tree[key]]
                if isinstance(tree[key], dict):
                    nested = tree.pop(key)
                    keys.extend(nested.keys())
                    tree.update(nested)
        except Exception:
            pass
        return []


class XmlaClient:
    """
    Class for communicate with ssas server with xmla over http
    """

    def __init__(self, config, auth):
        self.cfg = config
        self.auth = auth
        self.res = {}

    def __template(self, xmla_query: str) -> str:
        """
        Format request template

        :return: xmla request as str
        """

        return MsXmlaTemplates.REQUEST_TEMPLATE.format(xmla_query=xmla_query)  # type: ignore

    def discover(self, query: str) -> XmlaResponse:
        """
        Send DISCOVER xmla request with query

        :return: response ssas xmla server as XmlaResponse object
        """

        response = self.__request(
            data=self.__template(
                xmla_query=MsXmlaTemplates.QUERY_DISCOVER.format(query=query)  # type: ignore
            )
        )
        logger.info(response)
        return XmlaResponse(response)

    def execute(self, query, catalog_name=None) -> XmlaResponse:
        """
        Send EXECUTE xmla request with query

        :return: response ssas xmla server as XmlaResponse object
        """

        template = MsXmlaTemplates.QUERY_EXECUTE
        params = dict(
            query=query,
        )

        if catalog_name:
            template = MsXmlaTemplates.QUERY_EXECUTE_TO_CATALOG
            params["catalog_name"] = catalog_name

        response = self.__request(
            data=self.__template(xmla_query=template.format(**params))
        )

        return XmlaResponse(response)

    def __request(self, data) -> str:
        """
        Send request to server

        :return: server request as str
        """

        try:
            logger.info(f"REQ_AUTH - {self.auth}")

            res = requests.post(
                url=self.cfg.base_api_url,
                data=data.encode("utf-8"),
                auth=self.auth,
            )

            res.raise_for_status()

            res.encoding = "uft-8"

            if MsXmlaTemplates.ERROR_CODE in res.text:
                error_msg_body_start = re.search(MsXmlaTemplates.ERROR_CODE, res.text).span()[0] + 7
                error_msg_body_end = error_msg_body_start + re.search("Source=", res.text[error_msg_body_start:]).span()[0]
                raise XMLAServerResponseError(res.text[error_msg_body_start:error_msg_body_end])
            return res.text
        except XMLAServerResponseError as e:
            logger.error(f"XMLA error codes was received: {e}")
            raise e
        except Exception as excp:
            logger.error(f"Error occurred during sending '{data}' request to server: {excp}")
            raise
