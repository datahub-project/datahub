"""
Modul for reusable tools.
"""
from enum import Enum


class MsXmlaTemplates(str, Enum):
    """
    Queries templates.
    """

    REQUEST_TEMPLATE: str = """<Envelope xmlns="http://schemas.xmlsoap.org/soap/envelope/">
        <Header></Header>
        <Body>
            {xmla_query}
        </Body>
    </Envelope>"""
    QUERY_DISCOVER: str = """<Discover xmlns="urn:schemas-microsoft-com:xml-analysis">
        <RequestType>{query}</RequestType>
        <Restrictions>
            <RestrictionList>
                <ObjectExpansion>ExpandFull</ObjectExpansion>
            </RestrictionList>
        </Restrictions>
        <Properties>
        </Properties>
    </Discover>"""
    QUERY_EXECUTE: str = """<Execute xmlns='urn:schemas-microsoft-com:xml-analysis'>
        <Command>
             <Statement>
                {query}
            </Statement>
        </Command>
        <Properties>
        </Properties>
    </Execute>"""
    QUERY_EXECUTE_TO_CATALOG: str = """<Execute xmlns='urn:schemas-microsoft-com:xml-analysis'>
        <Command>
             <Statement>
                {query}
            </Statement>
        </Command>
        <Properties>
            <PropertyList>
                <Catalog>{catalog_name}</Catalog>
            </PropertyList>
        </Properties>
    </Execute>"""

    QUERY_METADATA: str = "DISCOVER_XML_METADATA"
    ERROR_CODE: str = "<Error ErrorCode"


class HostDefaults(str, Enum):
    """
    Host default values.
    """

    DOMAIN: str = ""
    SERVER: str = ""
    NAME: str = ""
    SEPARATOR: str = "."
