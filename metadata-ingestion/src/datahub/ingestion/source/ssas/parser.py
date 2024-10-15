"""
Module of parser class.
"""
from typing import Any, Dict

import xmltodict
from bs4 import BeautifulSoup as bs


class MdXmlaParser:
    """
    Multidimensional server parser
    """

    def get_server(self, xmla_str: str) -> Dict[str, Any]:
        """
        Get server data from xmla structure.

        :param xmla_str: string xmla data.
        :return: server data in dictionary.
        """
        bs_content = bs(xmla_str, "xml")
        return xmltodict.parse(str(bs_content.find("Server")))["Server"]
