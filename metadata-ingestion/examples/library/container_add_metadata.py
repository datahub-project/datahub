from datahub.emitter.mcp_builder import DatabaseKey
from datahub.sdk import ContainerUrn, CorpUserUrn, DataHubClient, DomainUrn, TagUrn

client = DataHubClient.from_env()

database_key = DatabaseKey(
    platform="snowflake",
    instance="production",
    database="analytics_db",
)

container = client.entities.get(ContainerUrn.from_string(database_key.as_urn()))

container.set_display_name("Analytics Database")
container.set_description(
    "Main analytics database containing reporting and metrics data"
)
container.set_subtype("Database")
container.set_external_url("https://app.snowflake.com/analytics_db")

container.set_tags([TagUrn("production"), TagUrn("analytics"), TagUrn("pii")])

container.set_terms(["urn:li:glossaryTerm:Finance.ReportingData"])

container.set_owners(
    [
        (CorpUserUrn("john.doe"), "DATAOWNER"),
        (CorpUserUrn("analytics-team"), "TECHNICAL_OWNER"),
    ]
)

container.set_domain(DomainUrn("Analytics"))

container.set_links(
    [
        (
            "https://wiki.company.com/analytics-db",
            "Database Documentation",
        ),
        (
            "https://jira.company.com/ANALYTICS-123",
            "Setup Ticket",
        ),
    ]
)

client.entities.update(container)

print(f"Updated container with comprehensive metadata: {container.urn}")
print(f"  - Tags: {len(container.tags or [])} tags")
print(f"  - Terms: {len(container.terms or [])} terms")
print(f"  - Owners: {len(container.owners or [])} owners")
print(f"  - Links: {len(container.links or [])} links")
print(f"  - Domain: {container.domain}")
