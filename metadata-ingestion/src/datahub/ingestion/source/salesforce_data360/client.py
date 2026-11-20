import logging
from typing import Dict, Iterable, List, Optional, Set, Union

from simple_salesforce import Salesforce

from datahub.ingestion.source.salesforce_data360.constants import (
    DMO_LIST_PATH,
    DMO_PAGE_SIZE,
    DMO_RELATIONSHIPS_PATH,
    MAX_DMO_PAGES,
    RELATIONSHIPS_PAGE_SIZE,
)
from datahub.ingestion.source.salesforce_data360.models import (
    DataModelObject,
    DmoRelationship,
)

logger = logging.getLogger(__name__)

HTTP_NO_CONTENT = 204
DMO_LIST_KEY = "dataModelObject"
RELATIONSHIPS_KEY = "relationships"
NEXT_PAGE_KEY = "nextPageUrl"


class SalesforceData360Client:
    def __init__(self, sf: Salesforce) -> None:
        self.sf = sf
        # Built from sf_version, not sf.base_url: the version is resolved after
        # the client is constructed (see SalesforceApi.update_salesforce_api_version).
        self.instance_root = f"https://{sf.sf_instance}"
        self.base_url = f"{self.instance_root}/services/data/v{sf.sf_version}/"

    def _get(
        self, url: str, params: Optional[Dict[str, Union[str, int]]] = None
    ) -> Dict:
        response = self.sf._call_salesforce("GET", url, params=params)
        # The relationships endpoint answers 204 with no body when a DMO has none.
        if response.status_code == HTTP_NO_CONTENT:
            return {}
        return response.json()

    def list_dmos(self) -> Iterable[DataModelObject]:
        url = self.base_url + DMO_LIST_PATH
        seen: Set[str] = set()
        for page_no in range(MAX_DMO_PAGES):
            entries: List[Dict] = self._get(
                url, {"limit": DMO_PAGE_SIZE, "offset": page_no * DMO_PAGE_SIZE}
            ).get(DMO_LIST_KEY, [])
            if entries and entries[0].get("name") in seen:
                logger.debug("DMO list repeated a page; stopping at offset paging")
                return
            for entry in entries:
                dmo = DataModelObject.model_validate(entry)
                if dmo.name not in seen:
                    seen.add(dmo.name)
                    yield dmo
            if len(entries) < DMO_PAGE_SIZE:
                return

    def list_relationships(
        self, dmo_name: str, dataspace: str
    ) -> Iterable[DmoRelationship]:
        url: Optional[str] = self.base_url + DMO_RELATIONSHIPS_PATH.format(
            dmo_name=dmo_name
        )
        params: Optional[Dict[str, Union[str, int]]] = {
            "dataspace": dataspace,
            "limit": RELATIONSHIPS_PAGE_SIZE,
        }
        while url:
            page = self._get(url, params)
            for entry in page.get(RELATIONSHIPS_KEY, []):
                yield DmoRelationship.model_validate(entry)
            next_page = page.get(NEXT_PAGE_KEY)
            # nextPageUrl is a server-relative path that already carries the query.
            url = self.instance_root + next_page if next_page else None
            params = None
