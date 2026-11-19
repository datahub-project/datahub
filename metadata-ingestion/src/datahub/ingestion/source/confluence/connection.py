from typing import Callable, Dict, List, Union

from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceCapability,
    TestConnectionReport,
)
from datahub.ingestion.source.confluence.client import (
    ConfluenceClient,
    create_atlassian_client,
)
from datahub.ingestion.source.confluence.config import (
    ConfluenceSourceConfig,
)
from datahub.ingestion.source.confluence.constants import PAGE_PROBE_EXPAND
from datahub.ingestion.source.confluence.report import (
    ConfluenceSourceReport,
)

SemanticSearchCheck = Callable[[ConfluenceSourceConfig], CapabilityReport]
CapabilityReports = Dict[Union[SourceCapability, str], CapabilityReport]

# test_connection samples a few configured items or spaces rather than
# crawling, so it stays fast on large sites.
_MAX_ITEMS_CHECKED = 5
_MAX_SPACES_SAMPLED = 3
_PAGES_PER_SAMPLE = 10
_DISCOVERY_SAMPLE_SIZE = 100

ACCESS_CAPABILITY = "Space/Page Access"
DISCOVERY_CAPABILITY = "Auto-Discovery"
SEMANTIC_SEARCH_CAPABILITY = "Semantic Search"


def run_connection_test(
    config_dict: dict, semantic_search_check: SemanticSearchCheck
) -> TestConnectionReport:
    """Validate the config and credentials, check access to the configured
    spaces and pages (or that spaces can be discovered), and check semantic
    search."""
    try:
        config = ConfluenceSourceConfig.model_validate(config_dict)
    except Exception as e:
        return TestConnectionReport(
            internal_failure=True,
            internal_failure_reason=f"Failed to parse config: {e}",
        )

    try:
        client = ConfluenceClient(
            create_atlassian_client(config), ConfluenceSourceReport()
        )
        try:
            client.list_spaces(start=0, limit=1)
        except Exception as e:
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(
                    capable=False,
                    failure_reason=f"Failed to connect to Confluence: {e.__class__.__name__}: {e}",
                    mitigation_message="Check that your credentials are correct and you have access to Confluence. "
                    "Verify the URL is correct (include /wiki for Cloud).",
                )
            )

        if config._parsed_space_allow or config._parsed_page_allow:
            capabilities = _check_configured_items(client, config)
        else:
            capabilities = _check_discovery(client, config)
        if all(report.capable for report in capabilities.values()):
            capabilities[SEMANTIC_SEARCH_CAPABILITY] = semantic_search_check(config)
        return TestConnectionReport(
            basic_connectivity=CapabilityReport(capable=True),
            capability_report=capabilities,
        )
    except Exception as e:
        return TestConnectionReport(
            internal_failure=True,
            internal_failure_reason=f"Unexpected error during connection test: {e}",
        )


def _check_configured_items(
    client: ConfluenceClient, config: ConfluenceSourceConfig
) -> CapabilityReports:
    inaccessible: List[str] = []
    accessible_pages = 0
    spaces = (config._parsed_space_allow or [])[:_MAX_ITEMS_CHECKED]
    page_ids = (config._parsed_page_allow or [])[:_MAX_ITEMS_CHECKED]

    for space_key in spaces:
        try:
            client.get_space(space_key)
        except Exception as e:
            inaccessible.append(f"space {space_key}: {e.__class__.__name__}")
            continue
        accessible_pages += _sample_page_count(client, space_key)

    for page_id in page_ids:
        try:
            if client.get_page(page_id, expand=PAGE_PROBE_EXPAND):
                accessible_pages += 1
        except Exception as e:
            inaccessible.append(f"page {page_id}: {e.__class__.__name__}")

    if inaccessible:
        more = "..." if len(inaccessible) > 3 else ""
        return {
            ACCESS_CAPABILITY: CapabilityReport(
                capable=False,
                failure_reason=f"Cannot access: {', '.join(inaccessible[:3])}{more}",
                mitigation_message="Verify your account has permission to access these Confluence spaces/pages. "
                "Check Space Settings > Permissions in Confluence.",
            )
        }
    return {
        ACCESS_CAPABILITY: CapabilityReport(
            capable=True,
            mitigation_message=f"Successfully validated access to {len(spaces) + len(page_ids)} configured item(s) "
            f"({len(spaces)} space(s), {len(page_ids)} page(s)). "
            f"Found ~{accessible_pages}+ accessible pages.",
        )
    }


def _check_discovery(
    client: ConfluenceClient, config: ConfluenceSourceConfig
) -> CapabilityReports:
    try:
        spaces = client.list_spaces(
            start=0, limit=min(config.max_spaces, _DISCOVERY_SAMPLE_SIZE)
        ).keys
    except Exception as e:
        return {
            DISCOVERY_CAPABILITY: CapabilityReport(
                capable=False,
                failure_reason=f"Failed to discover spaces: {e}",
                mitigation_message="Check that your account has permission to list Confluence spaces.",
            )
        }
    if not spaces:
        return {
            DISCOVERY_CAPABILITY: CapabilityReport(
                capable=False,
                failure_reason="No spaces found",
                mitigation_message="Your account may not have access to any Confluence spaces. "
                "Contact your Confluence administrator to grant space access.",
            )
        }

    pages_estimate = sum(
        _sample_page_count(client, space_key)
        for space_key in spaces[:_MAX_SPACES_SAMPLED]
    )
    limited = (
        "(limited by max_spaces config)" if len(spaces) >= config.max_spaces else ""
    )
    return {
        DISCOVERY_CAPABILITY: CapabilityReport(
            capable=True,
            mitigation_message=f"Discovered {len(spaces)} accessible space(s) {limited}. "
            f"Found ~{pages_estimate}+ pages in sampled spaces. "
            f"Auto-discovery will ingest all accessible content.",
        )
    }


def _sample_page_count(client: ConfluenceClient, space_key: str) -> int:
    # A page count is a hint for the user, so a failure here is not reported.
    try:
        return len(
            client.list_space_pages(
                space_key,
                start=0,
                limit=_PAGES_PER_SAMPLE,
                expand=PAGE_PROBE_EXPAND,
            ).pages
        )
    except Exception:
        return 0
