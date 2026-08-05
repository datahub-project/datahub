import logging
from typing import Dict, List, Optional, Type

import requests

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.confluent.config import ConfluentStreamCatalogConfig
from datahub.ingestion.source.confluent.constants import (
    DATA_KEY,
    ERRORS_KEY,
    MESSAGE_KEY,
)
from datahub.ingestion.source.confluent.models import CatalogEntityType

logger = logging.getLogger(__name__)


class ConfluentStreamCatalogClient:
    """
    Every failure is downgraded to a report warning rather than raised: the catalog
    needs Stream Governance and a role that grants catalog access, neither of which is
    guaranteed, and no source should fail its whole run over supplementary metadata.
    """

    def __init__(
        self,
        config: ConfluentStreamCatalogConfig,
        report: SourceReport,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.config = config
        self.report = report
        self.endpoint = config.get_graphql_endpoint()
        self.session = session or requests.Session()
        self.session.headers.update(
            {"Accept": "application/json", "Content-Type": "application/json"}
        )
        self.session.auth = config.get_credentials()

    def fetch_entities(
        self,
        query: str,
        root_key: str,
        model: Type[CatalogEntityType],
    ) -> List[CatalogEntityType]:
        """
        `root_key` is the GraphQL field the entities sit under, e.g. `kafka_topic`.
        Entities that fail to parse are skipped so one malformed record cannot cost
        the caller the whole page.
        """
        entities: List[CatalogEntityType] = []
        offset = 0

        while True:
            page = self._fetch_page(query, root_key, offset)
            if page is None:
                # Already reported; keep whatever paged in successfully so far.
                break

            for payload in page:
                entity = self._parse_entity(payload, root_key, model)
                if entity is not None:
                    entities.append(entity)

            if len(page) < self.config.page_size:
                break
            offset += self.config.page_size

        logger.info(
            f"Retrieved {len(entities)} {root_key} entities from the Confluent Stream Catalog"
        )
        return entities

    def _fetch_page(
        self, query: str, root_key: str, offset: int
    ) -> Optional[List[Dict[str, object]]]:
        # The live Confluent Cloud catalog endpoint returns HTTP 500 for any
        # operation that carries a GraphQL variables map (verified 2026-08-05),
        # so pagination arguments are inlined into the query text instead. Only
        # the {limit}/{offset} placeholders are substituted; both values are
        # integers, so no escaping is needed.
        inline_query = query.replace("{limit}", str(self.config.page_size)).replace(
            "{offset}", str(offset)
        )
        context = f"endpoint={self.endpoint}, entity={root_key}, offset={offset}"

        try:
            response = self.session.post(
                self.endpoint,
                json={"query": inline_query},
                timeout=self.config.timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
        except Exception as e:
            self.report.warning(
                message="Failed to query the Confluent Stream Catalog",
                context=context,
                exc=e,
            )
            return None

        if not isinstance(payload, dict):
            self.report.warning(
                message="Unexpected response from the Confluent Stream Catalog",
                context=context,
            )
            return None

        errors = payload.get(ERRORS_KEY)
        if errors:
            self.report.warning(
                message="The Confluent Stream Catalog returned GraphQL errors",
                context=f"{context}, errors={_summarise_errors(errors)}",
            )
            return None

        data = payload.get(DATA_KEY)
        if not isinstance(data, dict):
            return []

        entities = data.get(root_key)
        if not isinstance(entities, list):
            return []

        return [item for item in entities if isinstance(item, dict)]

    def _parse_entity(
        self,
        payload: Dict[str, object],
        root_key: str,
        model: Type[CatalogEntityType],
    ) -> Optional[CatalogEntityType]:
        try:
            return model.model_validate(payload)
        except Exception as e:
            self.report.warning(
                message="Skipping a Confluent Stream Catalog entity that could not be parsed",
                context=f"entity={root_key}, name={payload.get('name')}",
                exc=e,
            )
            return None

    def close(self) -> None:
        self.session.close()


def _summarise_errors(errors: object) -> str:
    if not isinstance(errors, list):
        return str(errors)
    messages = [
        str(error.get(MESSAGE_KEY, error)) if isinstance(error, dict) else str(error)
        for error in errors
    ]
    return "; ".join(messages)
