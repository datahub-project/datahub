"""
Recipe: Metric/KPI Data Discovery via Dashboard Lineage

Triggered when user asks for metrics or KPIs:
- "Calculate MAU for [customer]"
- "Generate SQL for revenue"
- "Count active users"
"""

RECIPE_ID = "metric-data-discovery"

RECIPE_XML = """\
  <recipe id="metric-data-discovery">
    <name>Metric/KPI Data Discovery via Dashboard Lineage</name>
    <applicability>
      Use when task requests SQL or data for METRICS or KPIs:
      - User metrics: "MAU", "DAU", "WAU", "active users", "user activity", "monthly active"
      - Business metrics: "revenue", "ARR", "MRR", "churn", "retention", "conversion"
      - Aggregation patterns: "total", "count of", "sum of", "average", "trend"
      - Time-based analysis: "monthly", "weekly", "daily", "rolling 30-day", "year-over-year"
      - Usage analytics: "usage events", "feature adoption", "engagement metrics"
      
      DO NOT use for:
      - Simple table lookups (e.g., "Find the customers table")
      - Schema questions without metrics (e.g., "What columns in orders?")
      - Impact/lineage analysis (e.g., "What breaks if...")
      
      PRIORITY: This recipe takes precedence over sql-generation when the query involves
      metrics/KPIs, because finding the RIGHT data source is more critical than SQL syntax.
    </applicability>
    
    <guidance>
      <principle>
        CRITICAL: For metric queries, the WRONG TABLE is the #1 failure mode.
        
        Example failure:
        - User asks: "Calculate MAU for Acme Corp"
        - Agent searches: "acme user activity" → Finds Elasticsearch raw events table
        - Result: Wrong! Should use analytics staging table (stg_datahub__due)
        
        WHY THIS HAPPENS:
        - Raw/operational tables often rank higher (more data, more queries)
        - Staging/mart tables have transformed, clean data but less obvious names
        - Elasticsearch tables are tenant-specific, not cross-tenant analytics
        
        THE SOLUTION: Trace from DASHBOARDS backward to find "blessed" analytics tables.
        If a dashboard already shows this metric, its upstream tables are validated sources.
      </principle>
      
      <principle name="separate-metric-from-entity">
        CRITICAL: Separate the METRIC TYPE from the ENTITY/CUSTOMER in your search.
        
        When user asks: "[metric] for [customer/entity]"
        - The METRIC TYPE determines which dashboard to find
        - The CUSTOMER/ENTITY determines the SQL filter (WHERE clause)
        
        Example: "Calculate MAU for Acme Corp"
        - METRIC TYPE: MAU (monthly active users) → search for "MAU" dashboards
        - CUSTOMER: Acme Corp → filter in SQL: WHERE namespace = 'acme-namespace-id'
        
        WRONG: Searching for "MAU acme dashboards" (too specific, won't find generic dashboards)
        RIGHT: Searching for "MAU" dashboards, then filtering for Acme in SQL
      </principle>
      
      <step0_find_metric_dashboards>
        Search for DASHBOARDS or CHARTS that show this METRIC TYPE (not customer-specific).
        
        CRITICAL: Search for the METRIC, NOT the customer/entity!
        - User asks: "Calculate MAU for [customer]"
        - WRONG search: "MAU [customer]" or "[customer] monthly active users"
        - RIGHT search: "MAU" or "monthly active users" or "Customer MAU"
        
        WHY: Dashboards are usually generic (e.g., "Customer MAU" shows all customers).
        The customer filter is applied in the SQL WHERE clause, not in finding the dashboard.
        
        Search strategy:
        - Extract the METRIC TYPE from the query (MAU, revenue, churn, etc.)
        - EXCLUDE customer/entity names from the dashboard search
        - PREFER smart_search when available - it provides AI-powered relevance ranking and
          returns DETAILED entity info (no need to call get_entities afterward):
          smart_search(
            semantic_query="dashboards showing {metric_type} metrics",
            keyword_search_query="/q {metric_keywords}",
            filters={"entity_type": ["DASHBOARD", "CHART"]}
          )
        - FALLBACK to search if smart_search unavailable:
          search(query="/q {metric_type_only}", filters={"entity_type": ["DASHBOARD", "CHART"]})
        - Example: For "MAU for [customer]" → "/q MAU OR monthly+active+users"
        - Example: For "revenue for [customer]" → "/q revenue"
        
        What to look for:
        - Generic dashboard titles: "Customer MAU", "User Analytics", "Usage Metrics", "Revenue Dashboard"
        - NOT customer-specific: dashboards named after the customer are rare
        - Prefer dashboards with clear production indicators
        
        IF dashboards found:
          → Store ALL relevant dashboard URNs (up to 3-4) in evidence: {"metric_dashboards": [...]}
          → Having multiple dashboards helps step1 identify the BEST source table
          → Proceed to step1_trace_upstream
        
        IF no dashboards found:
          → Note this in evidence: {"dashboards_found": false}
          → Proceed to step2_search_staging_tables (fallback path)
        
        done_when: "Found relevant dashboards (ideally 2-4 for cross-validation) showing the metric type OR confirmed no metric dashboards exist"
      </step0_find_metric_dashboards>
      
      <step1_trace_upstream>
        Get UPSTREAM lineage from dashboards to find source tables.
        
        EXPLORE MULTIPLE DASHBOARDS when step0 found several relevant ones:
        - If step0 found 2-4 relevant dashboards, trace lineage from ALL of them
        - This helps identify the BEST table candidate by seeing which tables are:
          * Used by multiple dashboards (higher confidence - multiple teams validate it)
          * At the right tier (staging/intermediate preferred over raw)
        - Tables appearing in lineage of multiple dashboards are likely the "canonical" source
        
        Use: get_lineage(urn=dashboard_urn, direction="upstream", max_hops=3)
        - Call this for each relevant dashboard from step0
        - Aggregate the results to find common source tables
        
        What to look for in lineage:
        - Staging tables (prefix: stg_, staging_)
        - Intermediate tables (prefix: int_, intermediate_)
        - Fact tables (prefix: fct_, fact_)
        - Mart tables (prefix: mart_, dim_)
        - Analytics tables (contains: analytics, metrics, aggregated)
        
        PRIORITIZATION ORDER (combine tier priority with dashboard count):
        1. Tables used by MULTIPLE dashboards at staging tier → BEST choice
        2. Tables used by MULTIPLE dashboards at any tier → High confidence
        3. Staging tables (stg_*) from single dashboard → Cleanest, most reliable
        4. Intermediate tables (int_*) → Pre-aggregated, ready for analysis
        5. Fact tables (fct_*) → Transactional grain, good for custom aggregations
        6. Raw tables → Only if no better option exists
        
        Store findings in evidence:
        {
          "source_tables": [
            {"urn": "...", "name": "stg_datahub__due", "tier": "staging", "dashboard_count": 3, "priority": 1},
            {"urn": "...", "name": "int_user_metrics", "tier": "intermediate", "dashboard_count": 2, "priority": 2},
            {"urn": "...", "name": "raw_events", "tier": "raw", "dashboard_count": 1, "priority": 6}
          ],
          "dashboards_explored": ["Dashboard A", "Dashboard B", "Dashboard C"]
        }
        
        done_when: "Traced upstream lineage from relevant dashboards and identified source tables with tier classification and usage frequency"
      </step1_trace_upstream>
      
      <step2_search_staging_tables>
        FALLBACK: If no dashboards found, search directly for staging/analytics tables.
        
        Search strategy:
        - PREFER smart_search when available - it provides AI-powered relevance ranking and
          returns DETAILED entity info including full schema (no need to call get_entities):
          smart_search(
            semantic_query="staging or analytics tables for {metric_type}",
            keyword_search_query="/q stg+{metric} OR staging+{metric} OR analytics+{metric}",
            filters={"entity_type": ["DATASET"]}
          )
        - FALLBACK to search if smart_search unavailable:
          - search(query="/q stg OR staging OR analytics {metric_keywords}")
          - search(query="/q int OR intermediate {metric_keywords}")
          - search(query="/q fct OR fact {metric_keywords}")
        
        For usage metrics specifically, look for:
        - Tables with "usage_event" or "due" (DataHub Usage Events) in the name
        - Tables in analytics_hub schema
        - Tables with namespace/tenant filtering capability
        
        AVOID:
        - Elasticsearch tables (tenant-specific, not cross-tenant analytics)
        - Raw event tables without transformation
        - Tables without clear time-series capability
        
        done_when: "Identified candidate analytics/staging tables through direct search"
      </step2_search_staging_tables>
      
      <step3_examine_transformation_sql>
        Examine the transformation SQL or sample queries to understand the data model.
        
        Use: get_dataset_queries(urn, source="MANUAL", count=10)
        
        What to extract:
        - Common filter patterns (namespace, tenant_id, date ranges)
        - Aggregation patterns (COUNT DISTINCT actor_urn for MAU)
        - JOIN patterns (which dimension tables to join)
        - Time window logic (DATEADD, INTERVAL, rolling windows)
        
        For cross-tenant metrics:
        - Look for namespace or tenant filtering patterns
        - Identify how to find the specific tenant/namespace
        - May need to search dim_customer or similar for namespace mapping
        
        Store patterns in evidence:
        {
          "query_patterns": {
            "date_filter": "DATEADD(day, -30, CURRENT_DATE())",
            "distinct_users": "COUNT(DISTINCT actor_urn)",
            "namespace_filter": "namespace = '{namespace_id}'"
          }
        }
        
        done_when: "Extracted query patterns from sample queries OR confirmed no queries available"
      </step3_examine_transformation_sql>
      
      <step4_generate_sql>
        Generate SQL using the discovered table and patterns.
        
        Use the highest-priority table from step1 or step2.
        Apply patterns learned from step3.
        
        For customer-specific queries (e.g., "MAU for [customer]"):
        - If namespace is needed, first look it up:
          * Search for customer in dim_customer or namespace mapping
          * Include a note about how to find namespace
        
        SQL structure should include:
        - Proper table reference (full qualified name)
        - Date filtering with appropriate syntax for the platform
        - User deduplication (COUNT DISTINCT actor_urn for MAU)
        - Any required tenant/namespace filtering
        
        done_when: "Generated SQL query using validated analytics table with proper patterns"
      </step4_generate_sql>
    </guidance>
    
    <expected_deliverable_requirements>
      The response MUST include:
      
      1. DATA DISCOVERY SUMMARY:
         - If multiple dashboards explored: "I traced upstream from [N] dashboards ([names]) and found..."
         - If single dashboard: "I traced upstream from [Dashboard Name] to find the source data"
         - If no dashboards: "I searched for staging/analytics tables since no dashboards exist for this metric"
      
      2. TABLE SELECTION RATIONALE:
         - "Using [table_name] (staging layer) because it contains cleaned, transformed usage events"
         - If validated across multiple dashboards: "This table appears in the lineage of [N] dashboards, 
           indicating it's the canonical source for this metric type"
         - Explain WHY this table over alternatives
      
      3. SQL QUERY:
         - Full SQL with proper date functions for the platform (DATEADD for Snowflake, etc.)
         - Include comments explaining key parts
      
      4. ASSUMPTIONS/NOTES:
         - Any namespace/tenant filtering needed
         - How to modify for different time windows
         - Alternative tables if they exist
      
      Example response structure (multiple dashboards):
      "I explored 3 dashboards related to user metrics: 'Customer MAU', 'Usage Analytics', and 
      'Monthly KPIs'. All three use `analytics_hub.public_staging.stg_datahub__due` as their 
      upstream source, which gives high confidence this is the canonical table for usage metrics.
      
      This staging table contains cleaned DataHub usage events with:
      - actor_urn (user identifier)
      - event_timestamp_utc (event time)
      - namespace (customer/tenant identifier)
      
      Here's the SQL for the customer's MAU:
      ```sql
      SELECT 
        COUNT(DISTINCT actor_urn) AS monthly_active_users
      FROM analytics_hub.public_staging.stg_datahub__due
      WHERE namespace = '<customer-namespace>'  -- Replace with customer's namespace
        AND event_timestamp_utc >= DATEADD(day, -30, CURRENT_DATE())
      ```
      
      Note: To find a customer's namespace, you can query `analytics_hub.public_core.dim_customer`."
    </expected_deliverable_requirements>
    
    <typical_workflow>
      <step>s0: Search for dashboards/charts showing the metric - filters={"entity_type": ["DASHBOARD", "CHART"]}. Capture 2-4 relevant dashboards if available.</step>
      <step>s1: If dashboards found, trace upstream lineage (max_hops=3) from EACH relevant dashboard. Identify tables used by multiple dashboards (highest confidence).</step>
      <step>s2: If no dashboards, search directly for staging/analytics tables (stg_, int_, fct_, analytics)</step>
      <step>s3: Examine sample queries from the best table candidate to learn patterns (date filters, aggregations)</step>
      <step>s4: Generate SQL using validated table and learned patterns</step>
    </typical_workflow>
    
    <examples>
      <example>
        Task: "Generate SQL to calculate MAU for [customer]"
        → s0: Search dashboards for "MAU" (NOT "MAU [customer]"!) → query="/q MAU OR monthly active users"
             Found 3 dashboards: "Customer MAU", "Usage Analytics", "Monthly Metrics"
        → s1: Trace upstream lineage from ALL 3 dashboards:
             - "Customer MAU" → stg_datahub__due, raw_datahub_due
             - "Usage Analytics" → stg_datahub__due, dim_customer
             - "Monthly Metrics" → stg_datahub__due, int_monthly_aggregates
             Result: stg_datahub__due appears in ALL 3 → HIGH CONFIDENCE choice
        → s3: Sample queries show: COUNT(DISTINCT actor_urn), namespace filter, DATEADD pattern
        → s4: Generated SQL with stg_datahub__due, namespace='<customer-namespace>', 30-day window
        → Response: "Traced from 3 dashboards (Customer MAU, Usage Analytics, Monthly Metrics). 
             All use stg_datahub__due as their source - this is the canonical table for usage metrics..."
      </example>
      <example>
        Task: "Write SQL to count distinct metadata aspects"
        → s0: Search dashboards for "metadata aspects" → Found "Weekly Active Users", "Aspect Trends"
        → s1: Trace upstream from both:
             - "Weekly Active Users" → stg_datahub__due, dim_customer
             - "Aspect Trends" → stg_datahub__due, dim_date
             Result: stg_datahub__due common to both
        → s3: Sample queries show join patterns, aspect counting logic
        → s4: Generated SQL for aspect counting
        → Response: "Both dashboards use stg_datahub__due as their source..."
      </example>
      <example>
        Task: "Get monthly revenue trend"
        → s0: Search dashboards for "revenue" → Found "Revenue Dashboard" (only 1 relevant)
        → s1: Upstream lineage (single dashboard) → Found fct_revenue, dim_customer, dim_time
        → s3: Sample queries show SUM(amount), GROUP BY month
        → s4: Generated SQL with proper time aggregation
        → Response: "Traced from Revenue Dashboard, using fct_revenue fact table..."
      </example>
      <example>
        Task: "Calculate DAU for all customers" (NO dashboards exist)
        → s0: Search dashboards → None found
        → s2: Search staging tables → Found stg_datahub__due, int_users_aggregated
        → s3: Sample queries reveal daily aggregation patterns
        → s4: Generated SQL with staging table
        → Response: "No existing dashboards found. Using stg_datahub__due staging table..."
      </example>
    </examples>
    
    <common_scenarios>
      <scenario name="mau-calculation">
        User asks: "Calculate MAU for [customer]"
        → Search for GENERIC dashboards: "/q MAU OR monthly active users" (NOT "MAU [customer]")
        → If multiple dashboards found, trace lineage from ALL of them
        → Tables appearing in multiple dashboard lineages = highest confidence
        → Apply [customer] filter in SQL WHERE clause (e.g., namespace = 'customer-id')
        → Key: Dashboard search is for METRIC TYPE, customer filter is in SQL
      </scenario>
      <scenario name="multiple-dashboards-pattern">
        Multiple relevant dashboards found (e.g., "Customer MAU", "Usage Analytics", "Monthly KPIs")
        → Trace lineage from each dashboard (up to 3-4)
        → Identify tables that appear in MULTIPLE lineage results
        → Tables used by multiple dashboards are "battle-tested" and likely canonical
        → Example: If stg_datahub__due appears upstream of 3 dashboards, it's highly reliable
      </scenario>
      <scenario name="usage-metrics">
        User asks: "How many users used [feature]?"
        → Use this recipe: Find feature usage dashboards (may find several)
        → Trace upstream from relevant ones → identify common source → filter by event_type
        → Key: Identify the event_type values for the feature
      </scenario>
      <scenario name="revenue-metrics">
        User asks: "What's the monthly revenue trend?"
        → Use this recipe: Find revenue dashboards → trace upstream → fct_revenue table
        → Key: Use proper time grouping, sum amount field
      </scenario>
      <scenario name="no-dashboard-fallback">
        No dashboards exist for this metric
        → Skip step1, go directly to step2
        → Search for staging/analytics tables directly
        → Be more cautious about table selection (explain uncertainty)
      </scenario>
      <scenario name="elasticsearch-trap">
        AVOID: Elasticsearch tables like "<cluster>"."<tenant>_datahub_usage_event"
        → These are tenant-specific raw events
        → Prefer Snowflake staging tables for cross-tenant analytics
        → Only use ES tables if specifically asked for real-time data
      </scenario>
    </common_scenarios>
  </recipe>"""
