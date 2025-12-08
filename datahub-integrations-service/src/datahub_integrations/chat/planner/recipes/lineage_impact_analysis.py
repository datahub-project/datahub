"""
Recipe: Lineage Impact Analysis - Rule of 10

Triggered when user asks about impact analysis or dependencies:
- "What would break if I delete users table?"
- "What depends on orders?"
- "Impact of deprecating dataset"
"""

RECIPE_NAME = "recipe-lineage-impact-analysis"

RECIPE_XML = """\
  <recipe name="recipe-lineage-impact-analysis">
    <name>Lineage Impact Analysis - Rule of 10</name>
    <applicability>
      Use when task requests:
      - Impact analysis (e.g., "What would break if I delete users table?", "What's affected by removing column?")
      - Downstream/upstream dependencies (e.g., "What depends on orders?", "What feeds into revenue_table?")
      - Deprecation/deletion impact (e.g., "Impact of deprecating dataset", "Tables affected by removal")
    </applicability>
    
    <guidance>
      <step0_search>
        Search for the source entity.
        
        done_when: "Search returned exactly 1 result (total=1 in search response)"
        return_to_user_when: "Search returned more than 1 result (total>1) - user must choose"
        on_fail: action='abort'
        
        If search total != 1: Mark status='returned_to_user', respond to user listing up to 10 matches, ask which one, end plan.
        Do NOT call revise_plan for returned_to_user steps - user input is needed!
      </step0_search>
      
      <step1_lineage>
        Get downstream lineage.
        
        Params:
        - entity_types: ["DATASET"] when user asks about "tables"
        - max_hops: 3
        - max_results: 30 for aggregation (facets give counts for ALL items server-side)
        - query: Use to filter lineage results (e.g., query="workspace.growthgestaofin" to find specific schema)
        
        Use query parameter when user asks about specific table in large lineage: helps find needle in haystack!
      </step1_lineage>
      
      <step2_present>
        CRITICAL: Structure the response with clear sections:
        
        1. STATE TOTAL: "{total} assets will be impacted"
        
        2. DIRECT IMPACT (degree 1): "{X} directly impacted"
           - Breakdown by platform/type for direct dependencies
           - Example: "5 datasets directly: Snowflake, Databricks, Trino, Redshift, BigQuery"
        
        3. INDIRECT IMPACT (degree 2+): "{Y} indirectly impacted"  
           - Breakdown by platform/type for indirect dependencies
           - Example: "52 indirectly: 30 Looker dashboards, 15 BigQuery tables, 7 PowerBI reports"
        
        Then apply Rule of 10:
        - If total ≤ 10: List all items explicitly with degree labels
        - If total > 10: Show aggregate counts as above, don't list individual items
      </step2_present>
      
    </guidance>
    
    <expected_deliverable_requirements>
      Impact analysis response structure (REQUIRED):
      
      1. Total: "{N} total assets will be impacted"
      2. Direct impact: "{X} directly impacted" + breakdown by platform/type
      3. Indirect impact: "{Y} indirectly impacted" + breakdown by platform/type
      4. Apply Rule of 10 for detail level
      
      The response MUST state explicit counts, not qualitative descriptions.
      Calculate direct/indirect by counting degree field in results.
    </expected_deliverable_requirements>
    
    <typical_workflow>
      <step>s0: Search - done_when="Found exactly 1", return_to_user_when="Found >1"</step>
      <step>s1: Get lineage with entity_types=["DATASET"], max_results=30</step>
      <step>s2: Present - total≤10: list all, total>10: aggregate from facets</step>
    </typical_workflow>
    
    <examples>
      <example>
        Found 1: Plan continues
        Found 3: Step fails → list 3, ask which one → plan ends
        Found 25: Step fails → list 10 best, ask which one → plan ends
      </example>
    </examples>
  </recipe>"""
