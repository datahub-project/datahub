"""
Recipe: SQL Query Generation with Ambiguity Handling

Triggered when user asks to generate SQL queries to retrieve data:
- "Write SQL to get monthly totals"
- "Generate query for top customers by revenue"
- "Create a query to find inactive users"
"""

RECIPE_NAME = "recipe-sql-generation"

RECIPE_XML = """\
  <recipe name="recipe-sql-generation">
    <name>SQL Query Generation with Ambiguity Handling</name>
    <applicability>
      Use when task requests SQL query generation to retrieve or analyze data:
      - Explicit SQL generation (e.g., "Write SQL to get monthly totals", "Generate query for revenue")
      - Data retrieval queries (e.g., "Show me top 10 customers", "Get orders from last month")
      - Query creation even with preliminary steps (e.g., "Find pet_profiles and write SQL query...")
      
      DO NOT use for:
      - Simple metadata queries (e.g., "What columns are in customers table?" - use list_schema_fields)
      - Schema inspection (e.g., "Show schema of orders" - use get_entities or list_schema_fields)
      - Impact/lineage analysis (e.g., "What breaks if I delete..." - use lineage-impact-analysis recipe)
    </applicability>
    
    <guidance>
      <principle>
        USE THIS RECIPE FOR SQL GENERATION TASKS (data retrieval queries).
        
        This includes tasks like:
        - "Write SQL query to..."
        - "Generate query for..."
        - "Show me [data]..." (where data needs to be queried)
        - Even if task has multiple parts like "Find tables and write SQL..." (the SQL part triggers this recipe)
        
        DO NOT use for simple metadata lookups:
        - "What columns are in..." → use list_schema_fields directly
        - "Show schema of..." → use get_entities or list_schema_fields directly
        
        This recipe handles BOTH simple cases (one table) and ambiguous cases (multiple tables with same name).
        
        TRANSPARENCY REQUIRED when ambiguity exists: If multiple tables match, explicitly acknowledge 
        this and explain the choice made. This builds user trust.
        
        CONTRAST WITH LINEAGE: For impact/lineage analysis, ambiguity should RETURN TO USER (see lineage-impact-analysis recipe).
        For SQL generation, we can make a reasonable choice and explain it.
      </principle>
      
      <step0_search>
        Search for the requested table/entity.
        
        PREFER smart_search when available - it provides AI-powered relevance ranking and
        returns DETAILED entity info including full schema (so you don't need to call get_entities):
          smart_search(
            semantic_query="find {table_name} table for {user_intent}",
            keyword_search_query="/q {table_name}",
            filters={"entity_type": ["DATASET"]}
          )
        
        FALLBACK to search if smart_search unavailable:
          search(query="/q {table_name}", filters={"entity_type": ["DATASET"]})
        
        done_when: "Search completed and returned results"
      </step0_search>
      
      <step1_check_ambiguity>
        Examine the search results to identify if multiple tables exist with the same or similar names.
        Look for same table name across different platforms (dbt, Snowflake, BigQuery, Redshift, etc.).
        
        IF only one table found:
          → Proceed directly to step2_generate_sql
        
        IF multiple tables found with same/similar name:
          → Proceed to step2_handle_ambiguity (transparency pattern)
        
        done_when: "Determined whether ambiguity exists"
      </step1_check_ambiguity>
      
      <step2_examine_query_patterns>
        OPTIONAL: Examine real query patterns to inform SQL generation.
        
        Use get_dataset_queries(urn, source="MANUAL", count=5-10) to retrieve user-written queries.
        This step is especially valuable for:
        - Complex queries (JOINs, aggregations, window functions)
        - Understanding common filter patterns
        - Learning which columns are frequently used together
        - Discovering typical date range logic
        
        What to look for in retrieved queries:
        - Common JOIN patterns: Which tables are joined? On what keys?
        - Aggregation logic: SUM, COUNT, AVG patterns
        - Filter patterns: WHERE clauses, date filters
        - Grouping strategies: GROUP BY columns
        - Column usage: Which columns appear most frequently?
        
        Skip this step if:
        - Query is very simple (e.g., "SELECT * FROM table")
        - No queries found (get_dataset_queries returns total=0)
        - Time constraints (user wants quick answer)
        
        done_when: "Retrieved sample queries OR confirmed none exist OR skipped for simplicity"
      </step2_examine_query_patterns>
      
      <step3_generate_sql>
        Generate the SQL query or provide schema information.
        
        IF only one table found in step1:
          → Generate SQL directly, no special handling needed
        
        IF multiple tables found in step1 (AMBIGUITY EXISTS):
          → Choose the FIRST search result (highest ranked by relevance)
          → Search ranking already considers usage patterns, recency, and popularity
          → Present to user WITH transparency using the REQUIRED RESPONSE STRUCTURE below
        
        REQUIRED RESPONSE STRUCTURE (for ambiguous cases only):
        1. ACKNOWLEDGE: "I found multiple [table_name] tables in your catalog:"
        2. LIST ALL OPTIONS with platforms:
           - "Snowflake: [qualified_name]"
           - "dbt: [qualified_name]"
           - "BigQuery: [qualified_name]"
           (etc.)
        3. STATE CHOICE: "I'll use the [platform] table (top-ranked result based on usage patterns)."
        4. PROVIDE ANSWER: [Execute the actual task - query, analysis, etc.]
        5. MENTION ALTERNATIVES: "Alternatively, you could query the [other_platform] version which [why it might be useful]."
        
        done_when: "SQL query generated AND delivered to user. If step evidence shows 'ambiguity_exists': true, verify response includes: (1) 'I found N tables' statement, (2) platform list with dashes, (3) 'I'll use X (top-ranked)' statement, (4) SQL query, (5) alternative mention. If no ambiguity, SQL query alone is sufficient."
      </step3_generate_sql>
    </guidance>
    
    <expected_deliverable_requirements>
      CRITICAL: The expected_deliverable you generate must be VERY SPECIFIC when this recipe applies.
      
      Use this template for the expected_deliverable field:
      
      "SQL query [that does X]. RESPONSE MUST CHECK: If search found multiple [table_name] tables 
      across platforms, response structure REQUIRED: (1) Start with 'I found N [table_name] tables:' 
      then list each as '- Platform: qualified.name', (2) State 'I'll use the [Platform] table 
      (top-ranked result)', (3) provide SQL query, (4) end with 'Note: You could also query the 
      [other_platform] version'. DO NOT just say 'using X table' - MUST acknowledge multiple exist first."
      
      Example for your expected_deliverable:
      "SQL query to get monthly pet profile creations. RESPONSE MUST CHECK: If search found multiple 
      pet_profiles tables, REQUIRED structure: (1) 'I found N pet_profiles tables:' then list each 
      as '- Snowflake: ...', '- dbt: ...', (2) 'I'll use [Platform] table (top-ranked)', (3) SQL query, 
      (4) 'Note: You could also query the dbt/Snowflake version'. DO NOT just say 'using Snowflake table'."
      
      This explicit structure in expected_deliverable ensures the executor LLM knows EXACTLY what to include.
      
      Response for ambiguous table queries MUST include ALL 5 elements:
      
      1. AMBIGUITY ACKNOWLEDGMENT: "I found [N] [table_name] tables" (NOT just "using the X table")
      2. PLATFORM LIST: Each option with platform marker:
         - "Snowflake: [qualified_name]"
         - "dbt: [qualified_name]"
      3. CHOICE STATEMENT: "I'll use the [platform] table (top-ranked result based on usage patterns)"
      4. SQL QUERY: The actual query requested
      5. ALTERNATIVES: "You could also query the [other_platform] version which [characteristic]"
      
      Example format:
      "I found 2 pet_profiles tables in your catalog:
      - Snowflake: long_tail_companions.adoption.pet_profiles
      - dbt: long_tail_companions.adoption.pet_profiles
      
      I'll use the Snowflake table (top-ranked result based on usage patterns). Here's the query:
      [SQL query]
      
      Note: You could also query the dbt model which may have additional transformations."
    </expected_deliverable_requirements>
    
    <typical_workflow>
      <step>s0: Search for the requested table/entity</step>
      <step>s1: Check search results for ambiguity (multiple tables with same/similar name)</step>
      <step>s2: Generate SQL - IF ambiguous, use transparency pattern (list all, choose first ranked, explain, provide SQL, mention alternatives). IF single result, generate SQL directly.</step>
    </typical_workflow>
    
    <examples>
      <example>
        Task: "Find tables related to pet profiles and write an SQL query to count pet profile creations by month"
        → THIS RECIPE APPLIES (task includes "write an SQL query")
        → Search finds: Snowflake pet_profiles (rank 1), dbt pet_profiles (rank 2)
        → Ambiguity detected → Use transparency pattern
        → Response: "I found 2 pet_profiles tables: Snowflake and dbt. Using Snowflake (top-ranked)... [SQL]... You could also use the dbt model."
      </example>
      <example>
        Task: "Write SQL to get monthly pet profile creations"
        → THIS RECIPE APPLIES (task includes "Write SQL")
        → Search finds: Snowflake pet_profiles (rank 1), dbt pet_profiles (rank 2)
        → Ambiguity detected → Use transparency pattern
        → Response: "I found 2 pet_profiles tables. Using Snowflake (top-ranked)... [SQL]... You could also use the dbt model."
      </example>
      <example>
        Task: "Show me the top 10 invoices by amount"
        → THIS RECIPE APPLIES (task requests data retrieval)
        → Search finds: Only 1 result (Snowflake invoices)
        → No ambiguity → Generate SQL directly
        → Response: "Here's the SQL query: SELECT * FROM invoices ORDER BY amount DESC LIMIT 10"
      </example>
    </examples>
    
    <common_scenarios>
      <scenario name="sql-generation">
        User asks: "Write SQL to get monthly totals from orders table"
        Multiple orders tables exist (dbt, Snowflake, BigQuery)
        → Use this recipe: Choose one, explain choice, mention alternatives
      </scenario>
      <scenario name="NOT-FOR-schema-metadata">
        User asks: "What columns are in the customers table?"
        → DO NOT use this recipe! This is a simple metadata query
        → Use list_schema_fields or get_entities directly (no planning needed)
        → If multiple tables exist, agent can handle ambiguity without complex recipe
      </scenario>
      <scenario name="NOT-FOR-impact">
        User asks: "What would break if I delete the users table?"
        Multiple users tables exist
        → DO NOT use this recipe! Use lineage-impact-analysis recipe which ABORTS and asks user to clarify
      </scenario>
      <scenario name="why-first-result">
        Always use the first (top-ranked) search result because:
        - Search already factors in usage frequency (most commonly queried tables rank higher)
        - Search considers recency and data quality signals
        - Deterministic and consistent behavior
        - User can always specify if they want a different version
      </scenario>
      <scenario name="transparency-is-key">
        Even though we pick first result automatically, we still list ALL options.
        This lets users know other versions exist and they can ask for those specifically.
        Example: User sees dbt option mentioned, can follow up with "use the dbt version instead"
      </scenario>
    </common_scenarios>
  </recipe>"""
