"""Generate Snowflake Cortex Agent SQL."""


def generate_cortex_agent_sql(
    agent_name: str,
    agent_display_name: str,
    agent_color: str,
    sf_warehouse: str | None,
    sf_database: str | None,
    sf_schema: str | None,
    include_mutations: bool = True,
) -> str:
    """Generate Cortex Agent SQL that uses configuration variables with DataHub tools.

    Args:
        agent_name: Agent name
        agent_display_name: Agent display name
        agent_color: Agent color
        sf_warehouse: Snowflake warehouse name (uses placeholder if None)
        sf_database: Snowflake database name (uses placeholder if None)
        sf_schema: Snowflake schema name (uses placeholder if None)
        include_mutations: Whether to include mutation/write tools (default: True)
    """
    # Use placeholders for None values - these will be set via SQL variables at runtime
    warehouse = sf_warehouse or "MY_WAREHOUSE"
    database = sf_database or "MY_DATABASE"
    schema = sf_schema or "MY_SCHEMA"

    # Build instructions based on whether mutations are enabled
    if include_mutations:
        system_capabilities = """- Search and discovery (search_datahub, search_documents)
      - Schema exploration (get_entities, list_schema_fields)
      - Lineage analysis (get_lineage, get_lineage_paths_between)
      - Query patterns (get_dataset_queries)
      - Data quality (get_dataset_assertions)
      - Metadata management (tags, descriptions, owners, domains, glossary terms)
      - User information (get_me)"""

        orchestration_guidance = """You are an assistant for business analytics and operational data questions for analysts, data engineers, and decision makers.

      You have access to:

      1) Snowflake data and SQL execution
      2) Enterprise metadata and knowledge via DataHub

      DataHub stores a map of the data supply chain across data tools, along with enterprise knowledge via indexed documents that may help clarify business definitions, processes, past decisions & more.

      Your goal is to combine both sources to correctly interpret user intent, identify the right datasets, generate reliable SQL, and provide clear answers.

      General rules:

      - Do not invent tables, columns, or business definitions.
      - Base SQL only on verified schema/context from available tools.
      - If requirements are ambiguous or key fields are missing, ask a concise clarifying question before executing SQL.
      - Prefer concise, practical responses with assumptions explicitly stated.
      - For SQL execution, use read-only queries unless the user explicitly requests otherwise.

      For data or analytics questions:
      1. Use DataHub tools first to find relevant datasets or documents.
      2. Use get_entities and/or list_schema_fields to validate schema details.
      3. Use get_dataset_queries when helpful to infer common query patterns.
      4. Generate Snowflake SQL based on verified schema.
      5. Execute the SQL, then summarize findings and caveats.

      For lineage questions:
      1. Use get_lineage for upstream/downstream exploration.
      2. Use get_lineage_paths_between for detailed end-to-end transformations.
      3. Explain lineage in plain business terms plus technical dependencies.

      For data quality questions:
      1. Use search_datahub to find the dataset and get its URN.
      2. Use get_dataset_assertions to fetch assertions and their run results.
      3. Summarize which checks are passing, failing, or erroring.
      4. Explain what each assertion checks in plain terms.

      For metadata management:
      1. Search entities first and collect exact URNs.
      2. Propose the intended changes clearly.
      3. Ask for explicit confirmation before any write action (tags, descriptions, owners, domains, glossary, structured properties).
      4. Execute only after confirmation and report what changed.

      If DataHub search returns no useful results:
      - Say so explicitly.
      - Ask for alternate names/business terms, or fallback to user-provided Snowflake table names."""
    else:
        system_capabilities = """- Search and discovery (search_datahub, search_documents)
      - Schema exploration (get_entities, list_schema_fields)
      - Lineage analysis (get_lineage, get_lineage_paths_between)
      - Query patterns (get_dataset_queries)
      - Data quality (get_dataset_assertions)
      - User information (get_me)"""

        orchestration_guidance = """You are an assistant for business analytics and operational data questions for analysts, data engineers, and decision makers.

      You have access to:

      1) Snowflake data and SQL execution
      2) Enterprise metadata and knowledge via DataHub

      DataHub stores a map of the data supply chain across data tools, along with enterprise knowledge via indexed documents that may help clarify business definitions, processes, past decisions & more.

      Your goal is to combine both sources to correctly interpret user intent, identify the right datasets, generate reliable SQL, and provide clear answers.

      General rules:

      - Do not invent tables, columns, or business definitions.
      - Base SQL only on verified schema/context from available tools.
      - If requirements are ambiguous or key fields are missing, ask a concise clarifying question before executing SQL.
      - Prefer concise, practical responses with assumptions explicitly stated.
      - For SQL execution, use read-only queries unless the user explicitly requests otherwise.

      For data or analytics questions:
      1. Use DataHub tools first to find relevant datasets or documents.
      2. Use get_entities and/or list_schema_fields to validate schema details.
      3. Use get_dataset_queries when helpful to infer common query patterns.
      4. Generate Snowflake SQL based on verified schema.
      5. Execute the SQL, then summarize findings and caveats.

      For lineage questions:
      1. Use get_lineage for upstream/downstream exploration.
      2. Use get_lineage_paths_between for detailed end-to-end transformations.
      3. Explain lineage in plain business terms plus technical dependencies.

      For data quality questions:
      1. Use search_datahub to find the dataset and get its URN.
      2. Use get_dataset_assertions to fetch assertions and their run results.
      3. Summarize which checks are passing, failing, or erroring.
      4. Explain what each assertion checks in plain terms.

      If DataHub search returns no useful results:
      - Say so explicitly.
      - Ask for alternate names/business terms, or fallback to user-provided Snowflake table names."""

    # Build mutation tools section if enabled
    mutation_tools = (
        """
    # Tag Management Tools
    - tool_spec:
        type: "generic"
        name: "add_tags"
        description: "Add tags to entities or columns. Confirm with user first before making changes."
        input_schema:
          type: "object"
          properties:
            tag_urns:
              type: "string"
              description: "JSON array of tag URNs (e.g., '[\\\"urn:li:tag:PII\\\"]')"
            entity_urns:
              type: "string"
              description: "JSON array of entity URNs"
            column_paths:
              type: "string"
              description: "JSON array of column names. Default: null (entity-level tagging)"
          required: [tag_urns, entity_urns, column_paths]

    - tool_spec:
        type: "generic"
        name: "remove_tags"
        description: "Remove tags from entities or columns. Confirm with user first."
        input_schema:
          type: "object"
          properties:
            tag_urns:
              type: "string"
              description: "JSON array of tag URNs to remove"
            entity_urns:
              type: "string"
              description: "JSON array of entity URNs"
            column_paths:
              type: "string"
              description: "JSON array of column names. Default: null (entity-level tag removal)"
          required: [tag_urns, entity_urns, column_paths]

    # Description Management
    - tool_spec:
        type: "generic"
        name: "update_description"
        description: "Update entity/column descriptions. Operations: 'replace', 'append', 'remove'. Confirm with user first."
        input_schema:
          type: "object"
          properties:
            entity_urn:
              type: "string"
              description: "Entity URN"
            operation:
              type: "string"
              description: "'replace', 'append', or 'remove'"
            description:
              type: "string"
              description: "Description text. Default: null (not needed for 'remove' operation)"
            column_path:
              type: "string"
              description: "Column name. Default: null (entity-level description)"
          required: [entity_urn, operation, description, column_path]

    # Domain Management
    - tool_spec:
        type: "generic"
        name: "set_domains"
        description: "Assign a domain to entities. Confirm with user first."
        input_schema:
          type: "object"
          properties:
            domain_urn:
              type: "string"
              description: "Domain URN (e.g., 'urn:li:domain:marketing')"
            entity_urns:
              type: "string"
              description: "JSON array of entity URNs"
          required: [domain_urn, entity_urns]

    - tool_spec:
        type: "generic"
        name: "remove_domains"
        description: "Remove domain assignments from entities. Confirm with user first."
        input_schema:
          type: "object"
          properties:
            entity_urns:
              type: "string"
              description: "JSON array of entity URNs"
          required: [entity_urns]

    # Owner Management
    - tool_spec:
        type: "generic"
        name: "add_owners"
        description: "Add owners to entities. Confirm with user first."
        input_schema:
          type: "object"
          properties:
            owner_urns:
              type: "string"
              description: "JSON array of owner URNs (CorpUser or CorpGroup)"
            entity_urns:
              type: "string"
              description: "JSON array of entity URNs"
            ownership_type_urn:
              type: "string"
              description: "Ownership type URN. Default: null (uses default ownership type)"
          required: [owner_urns, entity_urns, ownership_type_urn]

    - tool_spec:
        type: "generic"
        name: "remove_owners"
        description: "Remove owners from entities. Confirm with user first."
        input_schema:
          type: "object"
          properties:
            owner_urns:
              type: "string"
              description: "JSON array of owner URNs"
            entity_urns:
              type: "string"
              description: "JSON array of entity URNs"
            ownership_type_urn:
              type: "string"
              description: "Ownership type URN. Default: null (removes all ownership types)"
          required: [owner_urns, entity_urns, ownership_type_urn]

    # Glossary Term Management
    - tool_spec:
        type: "generic"
        name: "add_glossary_terms"
        description: "Add glossary terms to entities or columns. Confirm with user first."
        input_schema:
          type: "object"
          properties:
            term_urns:
              type: "string"
              description: "JSON array of glossary term URNs"
            entity_urns:
              type: "string"
              description: "JSON array of entity URNs"
            column_paths:
              type: "string"
              description: "JSON array of column names. Default: null (entity-level glossary terms)"
          required: [term_urns, entity_urns, column_paths]

    - tool_spec:
        type: "generic"
        name: "remove_glossary_terms"
        description: "Remove glossary terms from entities or columns. Confirm with user first."
        input_schema:
          type: "object"
          properties:
            term_urns:
              type: "string"
              description: "JSON array of glossary term URNs"
            entity_urns:
              type: "string"
              description: "JSON array of entity URNs"
            column_paths:
              type: "string"
              description: "JSON array of column names. Default: null (entity-level glossary terms)"
          required: [term_urns, entity_urns, column_paths]

    # Structured Property Management
    - tool_spec:
        type: "generic"
        name: "add_structured_properties"
        description: "Add structured properties to entities or columns. Confirm with user first."
        input_schema:
          type: "object"
          properties:
            property_values:
              type: "string"
              description: "JSON array of {{propertyUrn, value}} objects"
            entity_urns:
              type: "string"
              description: "JSON array of entity URNs"
            column_paths:
              type: "string"
              description: "JSON array of column names. Default: null (entity-level structured properties)"
          required: [property_values, entity_urns, column_paths]

    - tool_spec:
        type: "generic"
        name: "remove_structured_properties"
        description: "Remove structured properties from entities or columns. Confirm with user first."
        input_schema:
          type: "object"
          properties:
            property_urns:
              type: "string"
              description: "JSON array of property URNs to remove"
            entity_urns:
              type: "string"
              description: "JSON array of entity URNs"
            column_paths:
              type: "string"
              description: "JSON array of column names. Default: null (entity-level structured properties)"
          required: [property_urns, entity_urns, column_paths]
"""
        if include_mutations
        else ""
    )

    # Build mutation tool resources section if enabled
    mutation_tool_resources = (
        f"""
    # Tags
    add_tags:
      type: "function"
      execution_environment:
        type: "warehouse"
        warehouse: {warehouse}
      identifier: {database}.{schema}.ADD_TAGS

    remove_tags:
      type: "function"
      execution_environment:
        type: "warehouse"
        warehouse: {warehouse}
      identifier: {database}.{schema}.REMOVE_TAGS

    # Descriptions
    update_description:
      type: "function"
      execution_environment:
        type: "warehouse"
        warehouse: {warehouse}
      identifier: {database}.{schema}.UPDATE_DESCRIPTION

    # Domains
    set_domains:
      type: "function"
      execution_environment:
        type: "warehouse"
        warehouse: {warehouse}
      identifier: {database}.{schema}.SET_DOMAINS

    remove_domains:
      type: "function"
      execution_environment:
        type: "warehouse"
        warehouse: {warehouse}
      identifier: {database}.{schema}.REMOVE_DOMAINS

    # Owners
    add_owners:
      type: "function"
      execution_environment:
        type: "warehouse"
        warehouse: {warehouse}
      identifier: {database}.{schema}.ADD_OWNERS

    remove_owners:
      type: "function"
      execution_environment:
        type: "warehouse"
        warehouse: {warehouse}
      identifier: {database}.{schema}.REMOVE_OWNERS

    # Glossary Terms
    add_glossary_terms:
      type: "function"
      execution_environment:
        type: "warehouse"
        warehouse: {warehouse}
      identifier: {database}.{schema}.ADD_GLOSSARY_TERMS

    remove_glossary_terms:
      type: "function"
      execution_environment:
        type: "warehouse"
        warehouse: {warehouse}
      identifier: {database}.{schema}.REMOVE_GLOSSARY_TERMS

    # Structured Properties
    add_structured_properties:
      type: "function"
      execution_environment:
        type: "warehouse"
        warehouse: {warehouse}
      identifier: {database}.{schema}.ADD_STRUCTURED_PROPERTIES

    remove_structured_properties:
      type: "function"
      execution_environment:
        type: "warehouse"
        warehouse: {warehouse}
      identifier: {database}.{schema}.REMOVE_STRUCTURED_PROPERTIES
"""
        if include_mutations
        else ""
    )

    tool_count_note = (
        "21 tools (read + write)" if include_mutations else "10 tools (read-only)"
    )
    query_description = " and manage metadata" if include_mutations else ""
    comment_suffix = " and metadata management" if include_mutations else ""

    # Build sample questions based on whether mutations are enabled
    sample_questions_with_mutations = '''
      - question: "What tables contain customer data?"
        answer: "I'll search DataHub for datasets related to customer data."
      - question: "Show me the lineage for the sales_monthly table"
        answer: "I'll retrieve the lineage information for the sales_monthly table."
      - question: "Tag all PII datasets in the finance domain"
        answer: "I'll search for datasets in the finance domain and add PII tags to them."
      - question: "What queries use the users table?"
        answer: "I'll retrieve the SQL queries that reference the users table."
      - question: "Add a description to the revenue column"
        answer: "I'll update the description for the revenue column."
      - question: "Who owns the analytics datasets?"
        answer: "I'll search for analytics datasets and show their ownership information."
      - question: "Are there any failing data quality checks on the orders table?"
        answer: "I'll fetch the assertions for the orders table and check their status."'''

    sample_questions_readonly = '''
      - question: "What tables contain customer data?"
        answer: "I'll search DataHub for datasets related to customer data."
      - question: "Show me the lineage for the sales_monthly table"
        answer: "I'll retrieve the lineage information for the sales_monthly table."
      - question: "What queries use the users table?"
        answer: "I'll retrieve the SQL queries that reference the users table."
      - question: "Who owns the analytics datasets?"
        answer: "I'll search for analytics datasets and show their ownership information."
      - question: "Are there any failing data quality checks on the orders table?"
        answer: "I'll fetch the assertions for the orders table and check their status."'''

    sample_questions = (
        sample_questions_with_mutations
        if include_mutations
        else sample_questions_readonly
    )
    return f"""-- ============================================================================
-- Step 4: Create Cortex Agent with DataHub Tools
-- ============================================================================
-- This creates a Snowflake Cortex Agent that uses DataHub metadata
-- to generate accurate SQL queries{query_description}
--
-- Prerequisites:
-- - Run 00_configuration.sql first to set variables
-- - Run 01_network_rules.sql to set up network access
-- - Run 02_datahub_udfs.sql to create DataHub UDFs ({tool_count_note})
-- - Run 03_stored_procedure.sql to create EXECUTE_DYNAMIC_SQL
-- ============================================================================

USE DATABASE IDENTIFIER($SF_DATABASE);
USE SCHEMA IDENTIFIER($SF_SCHEMA);
USE WAREHOUSE IDENTIFIER($SF_WAREHOUSE);

CREATE OR REPLACE AGENT {agent_name}
  COMMENT = 'Agent that uses DataHub metadata for SQL generation{comment_suffix}'
  PROFILE = '{{"display_name": "{agent_display_name}", "color": "{agent_color}"}}'
  FROM SPECIFICATION
  $$
  models:
    orchestration: auto

  orchestration:
    budget:
      seconds: 60
      tokens: 32000

  instructions:
    response: |
      You are a business data assistant. Respond clearly and concisely for analysts, engineers, and decision makers.

      Response format:
      - Start with a direct answer in 1-3 sentences.
      - Then provide supporting details (key metrics, assumptions, caveats).
      - Include SQL only when it helps validate or reproduce the answer.
      - Use bullets/tables when comparing values or options.

      Tone and quality:
      - Be factual, practical, and concise.
      - State uncertainty explicitly and explain what is missing.
      - Never invent tables, columns, definitions, or lineage.
      - If the request is ambiguous, ask one focused clarifying question.

      Safety for metadata updates:
      - For any metadata mutation (tags, owners, descriptions, domains, glossary, structured properties), summarize the proposed change and request explicit confirmation before execution.
      - After execution, report exactly what changed.

    orchestration: |
      {orchestration_guidance}

    system: |
      You have comprehensive access to DataHub including:
      {system_capabilities}

    sample_questions:{sample_questions}

  tools:
    # Core Search & Discovery Tools
    - tool_spec:
        type: "generic"
        name: "search_datahub"
        description: "Search DataHub for entities (datasets, dashboards, etc.). Use /q prefix for structured queries. Returns URNs, names, descriptions, and metadata."
        input_schema:
          type: "object"
          properties:
            search_query:
              type: "string"
              description: "Search query (e.g., 'customer', '/q user+transaction')"
            entity_type:
              type: "string"
              description: "Entity type filter (e.g., 'dataset', 'tag', etc.). Default: null (all entity types)"
          required: [search_query, entity_type]

    - tool_spec:
        type: "generic"
        name: "get_entities"
        description: "Get detailed entity information including schema, tags, owners, lineage summary. Use URN from search results."
        input_schema:
          type: "object"
          properties:
            entity_urn:
              type: "string"
              description: "Entity URN from search results"
          required: [entity_urn]

    - tool_spec:
        type: "generic"
        name: "list_schema_fields"
        description: "List schema fields with filtering and pagination. Useful for large schemas or finding specific columns."
        input_schema:
          type: "object"
          properties:
            dataset_urn:
              type: "string"
              description: "Dataset URN"
            keywords:
              type: "string"
              description: "Keywords to filter fields (single string or JSON array). Default: null (no filtering)"
            limit:
              type: "number"
              description: "Max fields to return. Default: 100"
          required: [dataset_urn, keywords, limit]

    # Lineage Tools
    - tool_spec:
        type: "generic"
        name: "get_lineage"
        description: "Get upstream or downstream lineage for entities or columns. Returns lineage graph with metadata."
        input_schema:
          type: "object"
          properties:
            urn:
              type: "string"
              description: "Entity URN"
            column_name:
              type: "string"
              description: "Column name for column-level lineage. Default: null (entity-level lineage)"
            upstream:
              type: "number"
              description: "1 for upstream, 0 for downstream. Default: 1"
            max_hops:
              type: "number"
              description: "Max hops (1-3+). Default: 1"
            max_results:
              type: "number"
              description: "Max results. Default: 30"
          required: [urn, column_name, upstream, max_hops, max_results]

    - tool_spec:
        type: "generic"
        name: "get_lineage_paths_between"
        description: "Get detailed transformation paths between two entities/columns. Shows intermediate steps and queries."
        input_schema:
          type: "object"
          properties:
            source_urn:
              type: "string"
              description: "Source dataset URN"
            target_urn:
              type: "string"
              description: "Target dataset URN"
            source_column:
              type: "string"
              description: "Source column name. Default: null (dataset-level lineage)"
            target_column:
              type: "string"
              description: "Target column name. Default: null (dataset-level lineage)"
          required: [source_urn, target_urn, source_column, target_column]

    # Query Analysis Tools
    - tool_spec:
        type: "generic"
        name: "get_dataset_queries"
        description: "Get SQL queries that use a dataset/column. Filter by MANUAL (user queries) or SYSTEM (BI tools)."
        input_schema:
          type: "object"
          properties:
            urn:
              type: "string"
              description: "Dataset URN"
            column_name:
              type: "string"
              description: "Column name to filter queries. Default: null (queries for all columns)"
            source:
              type: "string"
              description: "'MANUAL', 'SYSTEM', or null for both. Default: null"
            count:
              type: "number"
              description: "Number of queries. Default: 10"
          required: [urn, column_name, source, count]

    # Data Quality Tools
    - tool_spec:
        type: "generic"
        name: "get_dataset_assertions"
        description: "Get data quality assertions for a dataset with their latest run results (pass/fail). Filter by column, type, or status. Use search_datahub first to find the dataset URN."
        input_schema:
          type: "object"
          properties:
            urn:
              type: "string"
              description: "Dataset URN (from search_datahub results)"
            column_name:
              type: "string"
              description: "Column/field path to filter assertions. Default: null (all assertions)"
            assertion_type:
              type: "string"
              description: "'FRESHNESS', 'VOLUME', 'FIELD', 'SQL', 'DATASET', 'DATA_SCHEMA', 'CUSTOM', or null for all"
            status:
              type: "string"
              description: "'PASSING', 'FAILING', 'ERROR', 'INIT', or null for all"
            count:
              type: "number"
              description: "Number of assertions to return. Default: 5"
            run_events_count:
              type: "number"
              description: "Recent run events per assertion (1-10). Default: 1"
          required: [urn, column_name, assertion_type, status, count, run_events_count]

    # Document Search Tools
    - tool_spec:
        type: "generic"
        name: "search_documents"
        description: "Search organization documents (runbooks, FAQs, knowledge articles from Notion, Confluence, etc.)."
        input_schema:
          type: "object"
          properties:
            search_query:
              type: "string"
              description: "Search query"
            num_results:
              type: "number"
              description: "Max results. Default: 10"
          required: [search_query, num_results]

    - tool_spec:
        type: "generic"
        name: "grep_documents"
        description: "Search within document content using regex patterns. Use after search_documents to find specific content."
        input_schema:
          type: "object"
          properties:
            urns:
              type: "string"
              description: "JSON array of document URNs"
            pattern:
              type: "string"
              description: "Regex pattern to search for"
            context_chars:
              type: "number"
              description: "Context characters. Default: 200"
            max_matches_per_doc:
              type: "number"
              description: "Max matches per document. Default: 5"
          required: [urns, pattern, context_chars, max_matches_per_doc]
{mutation_tools}
    # User Info
    - tool_spec:
        type: "generic"
        name: "get_me"
        description: "Get information about the authenticated user (profile, groups, privileges). This tool takes no parameters."
        input_schema:
          type: "object"
          properties: {{}}

    # SQL Executor
    - tool_spec:
        type: "generic"
        name: "SqlExecutor"
        description: "Execute SELECT SQL queries and return results. Use after generating SQL from DataHub metadata."
        input_schema:
          type: "object"
          properties:
            SQL_TEXT:
              type: "string"
              description: "SELECT SQL query (must start with SELECT)"
          required: [SQL_TEXT]

  tool_resources:
    # Search & Discovery
    search_datahub:
      type: "function"
      execution_environment:
        type: "warehouse"
        warehouse: {warehouse}
      identifier: {database}.{schema}.SEARCH_DATAHUB

    get_entities:
      type: "function"
      execution_environment:
        type: "warehouse"
        warehouse: {warehouse}
      identifier: {database}.{schema}.GET_ENTITIES

    list_schema_fields:
      type: "function"
      execution_environment:
        type: "warehouse"
        warehouse: {warehouse}
      identifier: {database}.{schema}.LIST_SCHEMA_FIELDS

    # Lineage
    get_lineage:
      type: "function"
      execution_environment:
        type: "warehouse"
        warehouse: {warehouse}
      identifier: {database}.{schema}.GET_LINEAGE

    get_lineage_paths_between:
      type: "function"
      execution_environment:
        type: "warehouse"
        warehouse: {warehouse}
      identifier: {database}.{schema}.GET_LINEAGE_PATHS_BETWEEN

    # Query Analysis
    get_dataset_queries:
      type: "function"
      execution_environment:
        type: "warehouse"
        warehouse: {warehouse}
      identifier: {database}.{schema}.GET_DATASET_QUERIES

    # Data Quality
    get_dataset_assertions:
      type: "function"
      execution_environment:
        type: "warehouse"
        warehouse: {warehouse}
      identifier: {database}.{schema}.GET_DATASET_ASSERTIONS

    # Documents
    search_documents:
      type: "function"
      execution_environment:
        type: "warehouse"
        warehouse: {warehouse}
      identifier: {database}.{schema}.SEARCH_DOCUMENTS

    grep_documents:
      type: "function"
      execution_environment:
        type: "warehouse"
        warehouse: {warehouse}
      identifier: {database}.{schema}.GREP_DOCUMENTS
{mutation_tool_resources}
    # User Info
    get_me:
      type: "function"
      execution_environment:
        type: "warehouse"
        warehouse: {warehouse}
      identifier: {database}.{schema}.GET_ME

    # SQL Executor
    SqlExecutor:
      type: "procedure"
      execution_environment:
        type: "warehouse"
        warehouse: {warehouse}
      identifier: {database}.{schema}.EXECUTE_DYNAMIC_SQL
  $$;

-- Grant usage to the specified role
GRANT USAGE ON AGENT {agent_name} TO ROLE IDENTIFIER($SF_ROLE);

-- Verify the agent was created
DESCRIBE AGENT {agent_name};

SELECT
    'Agent created successfully with {"21 DataHub tools (read + write)" if include_mutations else "10 DataHub tools (read-only)"}!' AS status,
    '{agent_name}' AS agent_name,
    'You can now use this agent in Snowflake Intelligence UI for {"SQL generation and metadata management" if include_mutations else "SQL generation and metadata exploration"}' AS next_steps;
"""
