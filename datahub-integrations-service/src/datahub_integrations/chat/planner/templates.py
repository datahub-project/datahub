"""
Plan templates for common task patterns.

Templates define reusable step structures that the planner LLM can use
to generate plans faster. Instead of generating all step details from scratch,
the LLM just provides the template ID and step overrides (tool + param_hints).

This reduces LLM output by ~50-60% compared to full plan generation.

Templates are based on analysis of 321 real customer queries (Nov-Dec 2025).
See experiments/query_categorization.md for the data-driven analysis.
"""

from datahub_integrations.chat.planner.models import OnFail, PlanTemplate, Step

# =============================================================================
# Template Definitions (Data-Driven)
# =============================================================================


DEFINITION_DISCOVERY_TEMPLATE = PlanTemplate(
    id="template-definition-discovery",
    name="Definition Discovery",
    description=(
        "Use when user asks for the definition or meaning of a term, metric, field, or concept. "
        "This pattern searches multiple sources (glossary, dashboards, datasets) to find "
        "comprehensive definitions. Covers ~12% of customer queries. "
        "Examples: 'what is the definition of MAU', 'what does market_type mean', "
        "'what is add to cart rate', 'what is turnover'."
    ),
    steps=[
        Step(
            id="s0",
            description="Search glossary terms for the definition",
            done_when="Glossary search completed, found terms or confirmed none exist",
            intent="search_glossary",
            on_fail=OnFail(action="continue"),
        ),
        Step(
            id="s1",
            description="Search dashboards and charts that may contain the definition",
            done_when="Dashboard/chart search completed",
            intent="search_dashboards",
            on_fail=OnFail(action="continue"),
        ),
        Step(
            id="s2",
            description="Search datasets with relevant documentation or descriptions",
            done_when="Dataset search completed",
            intent="search_datasets",
            on_fail=OnFail(action="continue"),
        ),
        Step(
            id="s3",
            description="Get detailed information from top results across all sources",
            done_when="Retrieved details from most relevant entities",
            intent="get_entity_details",
            on_fail=OnFail(action="revise"),
        ),
        Step(
            id="s4",
            description="Compile definition with source attribution",
            done_when="User received comprehensive definition with sources",
            intent="present_results",
        ),
    ],
    expected_deliverable_template="Definition of the term/metric with source attribution from glossary, dashboards, or datasets",
)


DATA_LOCATION_TEMPLATE = PlanTemplate(
    id="template-data-location",
    name="Data Location Discovery",
    description=(
        "Use when user asks where to find specific data or metrics. "
        "Searches datasets and dashboards to locate the requested information. "
        "Covers ~5% of customer queries. "
        "Examples: 'where can I find monthly churn rate by region', "
        "'where do I find inventory aging data', 'where could I find web signups for small business'."
    ),
    steps=[
        Step(
            id="s0",
            description="Search datasets for the requested data using keywords",
            done_when="Dataset search completed with results",
            intent="search_datasets",
            on_fail=OnFail(action="revise", hint="Try different keywords"),
        ),
        Step(
            id="s1",
            description="Search dashboards and charts that may contain the data",
            done_when="Dashboard search completed with results",
            intent="search_dashboards",
            on_fail=OnFail(action="continue"),
        ),
        Step(
            id="s2",
            description="Get details from top candidates (schema, sample queries, usage)",
            done_when="Retrieved details from most relevant sources",
            intent="get_entity_details",
            on_fail=OnFail(action="revise"),
        ),
        Step(
            id="s3",
            description="Present location options with recommendations",
            done_when="User received data location options with guidance on which to use",
            intent="present_results",
        ),
    ],
    expected_deliverable_template="Data location recommendations showing datasets and/or dashboards containing the requested information",
)


JOIN_DISCOVERY_TEMPLATE = PlanTemplate(
    id="template-join-discovery",
    name="Join Discovery",
    description=(
        "Use when user asks how to join two tables. "
        "Retrieves schemas and examines queries to identify join keys. "
        "Covers ~2% of customer queries. "
        "Examples: 'how do I join ACCOUNTS with ORGANIZATIONS', "
        "'how can I join EVENTS table with USERS', 'how do I establish relationship between partner_id and org_id'."
    ),
    steps=[
        Step(
            id="s0",
            description="Get schema for the first table",
            done_when="Retrieved schema with column names and types",
            intent="get_schema",
            on_fail=OnFail(action="abort"),
        ),
        Step(
            id="s1",
            description="Get schema for the second table",
            done_when="Retrieved schema with column names and types",
            intent="get_schema",
            on_fail=OnFail(action="abort"),
        ),
        Step(
            id="s2",
            description="Get sample queries that use both tables to identify join patterns",
            done_when="Retrieved queries showing how tables are joined OR confirmed no queries exist",
            intent="get_queries",
            on_fail=OnFail(action="continue"),
        ),
        Step(
            id="s3",
            description="Identify join keys and present SQL join example",
            done_when="User received join SQL with identified join keys",
            intent="present_results",
        ),
    ],
    expected_deliverable_template="SQL join example with identified join keys and explanation",
)


IMPACT_ANALYSIS_TEMPLATE = PlanTemplate(
    id="template-impact-analysis",
    name="Impact Analysis",
    description=(
        "Use when user asks about downstream impacts of changing/deleting an entity. "
        "Aligns with recipe-lineage-impact-analysis. Covers ~1% of customer queries. "
        "Examples: 'if I change X what breaks', 'what will be impacted if I deprecate Y', "
        "'what downstream assets depend on Z and who owns them'."
    ),
    steps=[
        Step(
            id="s0",
            description="Search for the source entity",
            done_when="Search returned exactly 1 result (total=1 in search response)",
            return_to_user_when="Search returned more than 1 result (total>1) - user must choose",
            intent="search_entity",
            on_fail=OnFail(action="abort"),
        ),
        Step(
            id="s1",
            description="Get downstream lineage to identify impacted assets",
            done_when="Retrieved downstream lineage showing dependent assets with degree information",
            intent="get_lineage",
            on_fail=OnFail(action="revise", hint="Try increasing max_hops"),
        ),
        Step(
            id="s2",
            description="Present impact analysis using Rule of 10 (if ≤10 items: list all, if >10: show aggregate counts by platform/type)",
            done_when="User received impact report with total count, direct/indirect breakdown, and platform distribution",
            intent="present_results",
        ),
    ],
    expected_deliverable_template="Impact analysis with total count, direct vs indirect breakdown, platform distribution. Apply Rule of 10 for detail level.",
)


SEARCH_THEN_EXAMINE_TEMPLATE = PlanTemplate(
    id="template-search-then-examine",
    name="Search Then Examine Details",
    description=(
        "Use when user wants to find entities and then get details about them. "
        "Examples: 'find PII datasets and show their owners', 'find tables with "
        "customer data and show their schema'."
    ),
    steps=[
        Step(
            id="s0",
            description="Search for entities matching criteria",
            done_when="Search returned matching entities",
            intent="search_entities",
            on_fail=OnFail(action="revise", hint="Try different search terms"),
        ),
        Step(
            id="s1",
            description="Retrieve detailed information for found entities",
            done_when="Details retrieved for relevant entities",
            intent="get_entity_details",
            on_fail=OnFail(action="revise"),
        ),
        Step(
            id="s2",
            description="Present combined results to user",
            done_when="User received entities with their details",
            intent="present_results",
        ),
    ],
    expected_deliverable_template="Search results with detailed information for each entity",
)


# =============================================================================
# Template Registry
# =============================================================================


PLAN_TEMPLATES: dict[str, PlanTemplate] = {
    DEFINITION_DISCOVERY_TEMPLATE.id: DEFINITION_DISCOVERY_TEMPLATE,
    DATA_LOCATION_TEMPLATE.id: DATA_LOCATION_TEMPLATE,
    JOIN_DISCOVERY_TEMPLATE.id: JOIN_DISCOVERY_TEMPLATE,
    IMPACT_ANALYSIS_TEMPLATE.id: IMPACT_ANALYSIS_TEMPLATE,
    SEARCH_THEN_EXAMINE_TEMPLATE.id: SEARCH_THEN_EXAMINE_TEMPLATE,
}


def get_template(template_id: str) -> PlanTemplate | None:
    """
    Get a plan template by ID.

    Args:
        template_id: The template identifier (e.g., "simple-search")

    Returns:
        The PlanTemplate if found, None otherwise
    """
    return PLAN_TEMPLATES.get(template_id)


def get_all_templates() -> list[PlanTemplate]:
    """
    Get all available plan templates.

    Returns:
        List of all registered PlanTemplate objects
    """
    return list(PLAN_TEMPLATES.values())


def get_template_descriptions() -> str:
    """
    Get a formatted string describing all available templates.

    This is used in the planner system prompt to help the LLM
    decide which template to use.

    Returns:
        Formatted string with template IDs and descriptions
    """
    lines = ["Available plan templates:"]
    for template in PLAN_TEMPLATES.values():
        lines.append(f"- {template.id}: {template.description}")
    return "\n".join(lines)
