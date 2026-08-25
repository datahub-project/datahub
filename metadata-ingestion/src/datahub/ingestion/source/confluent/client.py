import json
import logging
from collections import Counter
from dataclasses import dataclass
from typing import Dict, Generic, List, Optional, Type

import requests

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.confluent.config import ConfluentStreamCatalogConfig
from datahub.ingestion.source.confluent.constants import (
    DATA_KEY,
    ERRORS_KEY,
    LIMIT_PLACEHOLDER,
    MAX_CATALOG_PAGES,
    MAX_ERROR_BODY_CHARS,
    MESSAGE_KEY,
    OFFSET_PLACEHOLDER,
)
from datahub.ingestion.source.confluent.models import CatalogEntityType

logger = logging.getLogger(__name__)

_CREDENTIAL_REJECTED_STATUSES = (401, 403)

# Debug-only: cap the size of a sampled raw catalog payload we log so a single
# entity with large business metadata can't flood the log.
_MAX_DEBUG_PAYLOAD_CHARS = 4000


@dataclass(frozen=True)
class _CatalogPage:
    items: List[Dict[str, object]]
    raw_count: int


@dataclass(frozen=True)
class CatalogFetchResult(Generic[CatalogEntityType]):
    entities: List[CatalogEntityType]
    # False on a partial read (a page failed or the safety limit tripped): a missing
    # entity is then unknown, not absent, so callers must not treat it as authoritative.
    complete: bool


class ConfluentStreamCatalogClient:
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
    ) -> CatalogFetchResult[CatalogEntityType]:
        missing = [
            placeholder
            for placeholder in (LIMIT_PLACEHOLDER, OFFSET_PLACEHOLDER)
            if placeholder not in query
        ]
        if missing:
            # Missing {offset} loops forever; missing {limit} silently truncates.
            self.report.failure(
                message="Confluent Stream Catalog query is missing its pagination placeholders",
                context=f"entity={root_key}, missing={sorted(missing)}",
            )
            return CatalogFetchResult([], complete=False)

        entities: List[CatalogEntityType] = []
        offset = 0
        pages = 0
        complete = True

        # Debug aid: the catalog only returns the fields we query, so run with the
        # `datahub.ingestion.source.confluent` logger at DEBUG to see a sample raw
        # payload plus how many entities actually populate each field. That tells us
        # which relationships/attributes are worth mapping in a given environment
        # (e.g. `source_topic` populated on 0 vs. 120 topics).
        debug_enabled = logger.isEnabledFor(logging.DEBUG)
        field_population: "Counter[str]" = Counter()
        sample_logged = False

        while True:
            if pages >= MAX_CATALOG_PAGES:
                self.report.warning(
                    message="Stopped Confluent Stream Catalog pagination after hitting the "
                    "page safety limit; the catalog may be ignoring the offset parameter",
                    context=f"entity={root_key}, pages={pages}, entities_retrieved={len(entities)}, "
                    f"page_size={self.config.page_size}",
                )
                complete = False
                break

            page = self._fetch_page(query, root_key, offset)
            pages += 1
            if page is None:
                complete = False
                if entities:
                    self.report.warning(
                        message="Kept a partial Confluent Stream Catalog result after a page "
                        "failed to load, so some entities will be missing their catalog metadata",
                        context=f"entity={root_key}, entities_retrieved={len(entities)}, "
                        f"failed_at_offset={offset}",
                    )
                break

            if page.raw_count > len(page.items):
                # Dropped entries mean the result no longer mirrors the full catalog,
                # so callers must not treat a now-missing entity as authoritative.
                complete = False
                self.report.warning(
                    message="Skipped non-object entries in a Confluent Stream Catalog page",
                    context=f"entity={root_key}, offset={offset}, "
                    f"raw_count={page.raw_count}, object_count={len(page.items)}",
                )

            for payload in page.items:
                if debug_enabled:
                    if not sample_logged:
                        logger.debug(
                            f"Sample raw {root_key} Stream Catalog payload: "
                            f"{_truncate(json.dumps(payload, default=str))}"
                        )
                        sample_logged = True
                    _tally_populated_fields(payload, field_population)

                entity = self._parse_entity(payload, root_key, model)
                if entity is None:
                    complete = False
                    continue
                entities.append(entity)

            # raw_count, not filtered len(items): a non-object entry must not look like EOF.
            if page.raw_count < self.config.page_size:
                break
            offset += self.config.page_size

        logger.info(
            f"Retrieved {len(entities)} {root_key} entities from the Confluent Stream Catalog"
        )
        if debug_enabled and field_population:
            summary = ", ".join(
                f"{field}={count}/{len(entities)}"
                for field, count in sorted(field_population.items())
            )
            logger.debug(
                f"Confluent Stream Catalog {root_key} field population (entities with a "
                f"non-empty value): {summary}"
            )
        return CatalogFetchResult(entities, complete=complete)

    def _fetch_page(
        self, query: str, root_key: str, offset: int
    ) -> Optional[_CatalogPage]:
        inline_query = query.replace(
            LIMIT_PLACEHOLDER, str(self.config.page_size)
        ).replace(OFFSET_PLACEHOLDER, str(offset))
        context = f"endpoint={self.endpoint}, entity={root_key}, offset={offset}"

        try:
            response = self.session.post(
                self.endpoint,
                json={"query": inline_query},
                timeout=self.config.timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status in _CREDENTIAL_REJECTED_STATUSES:
                # A wrong key or an Essentials-tier environment makes the whole catalog
                # unreadable, so fail the run rather than bury it as one more warning.
                self.report.failure(
                    message="The Confluent Stream Catalog rejected the credentials; check the "
                    "Schema Registry API key has catalog read access on a Stream Governance "
                    "Advanced environment",
                    context=f"{context}, status={status}, response={_response_body(e)}",
                    exc=e,
                )
            else:
                self.report.warning(
                    message="The Confluent Stream Catalog rejected the request",
                    context=f"{context}, response={_response_body(e)}",
                    exc=e,
                )
            return None
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

        # Catalog metadata is supplementary, so an unusable response is a warning,
        # not a run-failing error: a Kafka ingestion must not fail because Confluent
        # reshaped the response or a proxy wrapped an error envelope in a 200.
        data = payload.get(DATA_KEY)
        if not isinstance(data, dict):
            self.report.warning(
                message="The Confluent Stream Catalog response is missing a data object",
                context=context,
            )
            return None

        if root_key not in data:
            self.report.warning(
                message="The Confluent Stream Catalog response is missing the queried field",
                context=f"{context}, fields_returned={sorted(data)}",
            )
            return None

        entities = data.get(root_key)
        if not isinstance(entities, list):
            self.report.warning(
                message="The Confluent Stream Catalog response field is not a list",
                context=f"{context}, field={root_key}",
            )
            return None

        items = [item for item in entities if isinstance(item, dict)]
        return _CatalogPage(items, len(entities))

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


def _truncate(text: str) -> str:
    if len(text) <= _MAX_DEBUG_PAYLOAD_CHARS:
        return text
    return f"{text[:_MAX_DEBUG_PAYLOAD_CHARS]}… (truncated)"


def _tally_populated_fields(
    payload: Dict[str, object], field_population: "Counter[str]"
) -> None:
    # A field counts as populated when it is present and not null/empty — an empty
    # list (the catalog's shape for an unset relationship) does not count.
    for field, value in payload.items():
        if value in (None, "", [], {}):
            continue
        field_population[field] += 1


def _response_body(error: requests.HTTPError) -> str:
    if error.response is None:
        return ""
    return error.response.text[:MAX_ERROR_BODY_CHARS]


def _summarise_errors(errors: object) -> str:
    if not isinstance(errors, list):
        return str(errors)
    messages = [
        str(error.get(MESSAGE_KEY, error)) if isinstance(error, dict) else str(error)
        for error in errors
    ]
    return "; ".join(messages)
