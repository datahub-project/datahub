"""
Recipe library for planning tasks.

Recipes provide guidance to the planner LLM on how to approach common task patterns.
"""


def get_recipe_guidance() -> str:
    """
    Get recipe guidance as XML for the planner system prompt.

    Returns recipes in XML format that can be included in a separate system message.
    This allows programmatic construction while keeping the main prompt clean.
    """
    return """
<recipes>
  <recipe id="pii-curated-entities">
    <name>PII Discovery - Requires Explicit Tagging</name>
    <applicability>
      Use when task requests finding PII/sensitive data entities:
      - "Show datasets containing PII"
      - "Find tables with personal information"
      - "Find jobs that access PII"
      - "Find jobs that process sensitive data"
      - "Show pipelines processing sensitive data"
      - "Which dashboards display PII?"
      
      Entity types: DATASET, DATA_JOB, DATA_FLOW, DASHBOARD, CHART, ML_MODEL, etc.
    </applicability>
    
    <constraints>
      CRITICAL: Your plan's constraints.tool_allowlist MUST be exactly: ["search", "get_entities", "respond_to_user"]
      
      DO NOT include in tool_allowlist:
      - get_lineage (lineage inference not allowed for PII compliance)
      - get_dataset_queries (not needed for this task)
      - Any other tools
      
      Example constraints object in your plan:
      {
        "tool_allowlist": ["search", "get_entities", "respond_to_user"],
        "max_tool_calls": 10
      }
    </constraints>
    
    <guidance>
      <principle>
        COMPLIANCE REQUIREMENT: ALL PII entities MUST be explicitly tagged. NO keyword search, NO lineage inference.
        
        WHY NO LINEAGE:
        - For compliance/audit, only EXPLICITLY tagged entities count
        - Lineage inference is unreliable (jobs may read datasets but not access PII columns)
        - Lineage inference is not acceptable for regulatory reporting
        - If jobs aren't tagged, that's a governance gap to REPORT, not work around
        
        This recipe enforces: "If it's not tagged, it doesn't exist for compliance purposes."
        Missing tags = governance failure that must be surfaced to users, not hidden by clever inference.
        
        Return-to-user scenarios: (1) No PII metadata on [entity_type], (2) PII metadata exists but no [entity_type] tagged.
      </principle>
      
      <step0_facet_exploration>
        FACET EXPLORATION: search(query="*", filters={"entity_type": [user-requested types]}, num_results=0)
        
        This returns ONLY facets (no search results). Examine facets for PII patterns:
        - tags facet: pii, accesses_pii, processes_pii, sensitive, confidential, gdpr, hipaa (+ org-specific)
        - glossaryTerms facet: PII, PersonalData, SensitiveData, Classification.* (+ org-specific)
        - domain facet: Privacy, Compliance
        
        Construct URNs from facet names and store in evidence: {"pii_tags": [...], "pii_terms": [...]}
        
        done_when: "Facet exploration found PII metadata, extracted URNs"
        return_to_user_when: "Facet exploration found NO PII-related tags/glossary terms on [entity_type]"
      </step0_facet_exploration>
      
      <step1_filter_by_metadata>
        Extract URNs from s0 evidence → search(query="*", filters={"and": [{"entity_type": [...]}, {"or": [{"tag": [URNs]}, {"glossary_term": [URNs]}]}]}, num_results=30)
        
        CRITICAL: query="*" (not "/q pii"). Use URNs from s0 evidence in filters.
        
        IF search returns total=0 → RETURN TO USER: "PII metadata exists but no [entity_type] tagged. Must tag [entity_type]."
        IF search returns total>0 → Return results
        
        done_when: "Search using URN filters returned results (total > 0)"
        return_to_user_when: "Search returned 0 results"
      </step1_filter_by_metadata>
    </guidance>
    
    <expected_deliverable_requirements>
      The expected_deliverable in the plan MUST emphasize explicit tagging requirement:
      
      "List of [entity_type] EXPLICITLY TAGGED with PII metadata. For compliance purposes, only tagged entities count - lineage inference is not acceptable. If no tagged entities found, report governance gap."
      
      Response templates:
      - Success: "Found X [entity_type] explicitly tagged with PII metadata: [list]"
      - Return to user (s0): "No PII tags on [entity_type]. For compliance, [entity_type] accessing PII must be explicitly tagged."
      - Return to user (s1): "PII tags exist but no [entity_type] tagged. Governance gap: [entity_type] accessing PII must be explicitly tagged."
    </expected_deliverable_requirements>
    
    <typical_workflow>
      <step>s0: "Facet Exploration" - search(query="*", filters={"entity_type": [requested]}, num_results=0) → examine facets → identify PII metadata → extract URNs. If NONE → RETURN TO USER. Else store.</step>
      <step>s1: "Filter by PII metadata" - Extract URNs from s0 → search with URN filters. If total=0 → RETURN TO USER. If >0 → respond.</step>
    </typical_workflow>
    
    <param_hints_for_planner>
      CRITICAL - Plan assumptions MUST include:
      - "For compliance, only explicitly tagged [entity_type] count - no lineage inference"
      - "This is the ONLY acceptable approach for regulatory reporting - explicit tags only"
      - "If entities aren't tagged, report the governance gap - do NOT work around with lineage"
      - "This plan should NOT be revised to add lineage or keyword searches"
      
      CRITICAL - Plan goal MUST emphasize:
      - "Find [entity_type] EXPLICITLY TAGGED with PII metadata"
      - NOT "find [entity_type] that access PII data" (implies lineage)
      - NOT "identify [entity_type] that process PII" (implies inference)
      
      Step s0 - "Facet Exploration": 
        description: "Facet exploration to discover PII metadata on [entity_type]"
        tool: "search"
        param_hints: {
          "query": "*",
          "filters": {"entity_type": [user-requested types like "DATA_JOB", "DATASET", etc.]},
          "num_results": 0
        }
        on_fail: {"action": "abort"}
        return_to_user_when: "No PII metadata found in facets"
      
      Step s1 - "Filter by PII metadata":
        description: "Search [entity_type] using PII metadata URNs from s0"
        tool: "search"
        param_hints: {
          "query": "*",
          "filters": {
            "and": [
              {"entity_type": [same as s0]},
              {"or": [
                {"tag": ["<EXTRACT_FROM_S0_pii_tags>"]},
                {"glossary_term": ["<EXTRACT_FROM_S0_pii_terms>"]}
              ]}
            ]
          },
          "num_results": 30
        }
        on_fail: {"action": "abort"}
        return_to_user_when: "Search returned 0 results"
    </param_hints_for_planner>
    
    <examples>
      <example>"Find PII datasets" → s0: facets show "pii"/"Confidential" tags → s1: 45 datasets found → "Found 45 PII datasets"</example>
      <example>"Find PII jobs" → s0: facets show "accesses_pii" tag → s1: 12 jobs found → "Found 12 jobs"</example>
      <example>"Show PII dashboards" → s0: NO PII tags in facets → RETURN TO USER at s0 → "No PII tags on dashboards. Must tag."</example>
      <example>"Find sensitive pipelines" → s0: tags exist → s1: total=0 → RETURN TO USER at s1 → "Tags exist but no pipelines tagged."</example>
    </examples>
  </recipe>

  <recipe id="lineage-impact-analysis">
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
  </recipe>

  <recipe id="classification-tier-discovery">
    <name>Classification and Tier Discovery</name>
    <applicability>
      Use when task requests:
      - Finding assets by tier/classification (e.g., "How many assets in gold tier?", "Show platinum datasets")
      - Categorization queries (e.g., "Which tables are marked as deprecated?", "Datasets labeled as PII")
      - Quality level discovery (e.g., "Find certified datasets", "Show data quality tier breakdown")
    </applicability>
    
    <guidance>
      <principle>
        CRITICAL: Classification terms (like "platinum", "certified", "PII") can exist as:
        1. Tags (urn:li:tag:...)
        2. Glossary Terms (urn:li:glossaryTerm:...)
        3. Other metadata
        
        Do NOT assume which! Investigate first.
      </principle>
      
      <step0_discover>
        Discover what classification mechanisms exist using facet-based exploration.
                
        Strategy (facet-based discovery):
        
        Search for all assets (or specific entity type if user specified):
        - Use: search(query="*", count=0)
        - OR: search(query="*", entity_types=["DATASET"], count=0) if user asked about datasets specifically
        - This returns facets showing ALL metadata applied to assets:
          * tags facet: ALL tags in use (examine for classification patterns)
          * glossaryTerms facet: ALL glossary terms in use
          * platform, domain, customProperties facets
        
        Examine facets to identify classification-related metadata:
        - From tags facet, look for classification patterns matching user's query:
          * Example: User asks "premium tier" → look for "premium", "premium_tier", "tier_premium", "platinum", "gold", "tier1", etc.
          * Be inclusive: look for synonyms and variations
          * Organization-specific: "ltc_premium", "t1_certified", etc.
        
        - From glossaryTerms facet, look for matching classification terms:
          * "Premium", "PremiumTier", "Classification.Premium"
          * Synonyms: "Platinum", "Gold", "Tier1"
        
        - From customProperties facet or additional searches:
          * tier=premium, classification=premium, data_quality=premium
        
        Extract URNs of ALL matching classification metadata:
        - Construct URNs from facet names:
          * Tag "premium" → "urn:li:tag:premium"
          * Tag "premium_tier" → "urn:li:tag:premium_tier"
          * Glossary "Premium" → "urn:li:glossaryTerm:Premium"
        
        Store in evidence:
        Example: {
          "classification_tags": ["urn:li:tag:premium", "urn:li:tag:premium_tier"],
          "classification_terms": ["urn:li:glossaryTerm:Premium"]
        }
                
        done_when: "Explored asset facets to discover all tags/glossary terms, identified those matching the classification term, extracted URNs"
      </step0_discover>
      
      <step1_count_each>
        For EACH classification mechanism found:
        - Count assets using that tag
        - Count assets using that glossary term
        - Compare the counts
        
        Do NOT pick one without checking all candidates!
      </step1_count_each>
      
      <step2_present_breakdown>
        If multiple mechanisms found with different counts:
        - Present ALL options with counts
        - "I found [term] used in multiple ways: Tag (X assets), Glossary Term (Y assets)"
        - Answer based on context or ask user which they meant
      </step2_present_breakdown>
    </guidance>
    
    <typical_workflow>
      <step>s0: Facet-based discovery - search(query="*", count=0) → examine facets (tags, glossaryTerms) → identify classification-related ones → extract URNs</step>
      <step>s1: For each URN found, count assets - search with tag filter (count=0 for facets), search with glossary filter (count=0), compare counts</step>
      <step>s2: Present breakdown - if multiple mechanisms exist with different counts, present ALL options with counts, answer based on context or ask for clarification</step>
    </typical_workflow>
    
    <examples>
      <example>
        Task: "How many assets in premium tier?"
        → Step 0: Facet exploration - search(query="*", count=0) → examine facets → tags facet shows "premium", "premium_tier", "platinum" → glossaryTerms facet shows "Premium" → extract URNs
        → Step 1: Count each - search(filters={"tags": ["urn:li:tag:premium"]}, count=0) → 25 assets, search(filters={"tags": ["urn:li:tag:premium_tier"]}, count=0) → 30 assets, search(filters={"glossaryTerms": ["urn:li:glossaryTerm:Premium"]}, count=0) → 25 assets
        → Step 2: Present: "Found 'premium' used in multiple ways: tag:premium (25 assets), tag:premium_tier (30 assets), glossaryTerm:Premium (25 assets). Likely tag:premium and glossaryTerm:Premium refer to the same 25 assets."
      </example>
      <example>
        Task: "Show platinum datasets"
        → Step 0: Facet exploration - search(query="*", entity_types=["DATASET"], count=0) → examine facets → tags facet shows "platinum" → no matching glossary terms
        → Step 1: Count via tag:platinum - search(query="*", filters={"tags": ["urn:li:tag:platinum"]}, entity_types=["DATASET"], count=0) → 15 datasets
        → Step 2: Present: "Found 15 datasets with tag:platinum"
      </example>
    </examples>
  </recipe>

  <recipe id="broad-data-discovery">
    <name>Broad Data Discovery with Metadata Investigation</name>
    <applicability>
      Use when task requests:
      - Finding all data related to a topic (e.g., "What data do we have about pets?", "Show me customer-related assets")
      - Exploratory data discovery (e.g., "What datasets contain financial information?", "Find all marketing data")
      - Topic-based searches (e.g., "Show me everything related to orders", "What data exists for product analytics?")
      
      DO NOT use for:
      - Specific entity lookups (e.g., "Find the orders table") - use simple search instead
      - Impact/lineage analysis (e.g., "What breaks if...") - use lineage-impact-analysis recipe
      - SQL generation tasks - use sql-generation recipe
    </applicability>
    
    <guidance>
      <principle>
        CRITICAL: Before doing broad keyword searches, first investigate what metadata structures exist.
        
        The organization may have already organized data related to the topic using:
        1. Tags (e.g., tag:pets, tag:customer-data, tag:financial)
        2. Glossary Terms (e.g., business definitions for "Customer", "Revenue")
        3. Structured Properties (e.g., data_category:pets, sensitivity:pii)
        4. Domains (e.g., "Pet Services" domain, "Finance" domain)
        
        If these exist, they provide MUCH BETTER results than keyword matching:
        - More precise (curated by data stewards)
        - Better coverage (finds datasets without keywords in names)
        - Less noise (avoids false positives from keyword matching)
        - Faster execution (filtered searches are more efficient)
      </principle>
      
      <step0_investigate_metadata>
        Investigate what metadata structures exist for this topic using facet-based discovery.
                
        Search strategy (facet-based discovery):
        
        Search for all assets to get their metadata facets:
        - Use: search(query="*", count=0)
        - OR if user specified entity types: search(query="*", entity_types=["DATASET"], count=0)
        - This returns facets showing ALL metadata applied to assets:
          * tags facet: ALL tags used (e.g., "pets", "animals", "customer", "financial", etc.)
          * glossaryTerms facet: ALL glossary terms applied
          * platform facet: ALL platforms
          * domain facet: ALL domains
          * customProperties facet: May show structured properties
        
        Examine the facets to identify topic-related metadata:
        - From tags facet, look for topic patterns:
          * Direct matches: "pets", "pet_data", "animal_care"
          * Synonyms: "animals", "companions", "wildlife"
          * Related concepts: "veterinary", "adoption", "breeding"
          * Organization-specific: "ltc_pets", "petco_animals", etc.
        
        - From glossaryTerms facet, look for topic patterns:
          * Business terms: "PetData", "AnimalRecords", "CustomerPets"
          * Domain-specific: "VeterinaryServices", "AdoptionMetrics"
        
        - From domain facet: "Pet Services", "Animal Care", "Veterinary", etc.
        
        - From customProperties facet: category=pets, data_domain=animals, etc.
        
        Extract URNs of ALL identified topic-related metadata:
        - The facets show tag/glossary names, construct URNs:
          * Tag name "pets" → "urn:li:tag:pets"
          * Tag name "animals" → "urn:li:tag:animals"
          * Glossary term "PetData" → "urn:li:glossaryTerm:PetData"
          * Domain "Pet Services" → "urn:li:domain:PetServices"
        
        Store in evidence for step1:
        Example: {
          "topic_tags": ["urn:li:tag:pets", "urn:li:tag:animals", "urn:li:tag:pet_care"],
          "topic_terms": ["urn:li:glossaryTerm:PetData"],
          "topic_domains": ["urn:li:domain:PetServices"]
        }
        
        done_when: "Explored asset facets to discover all tags/glossary/domains in use, identified topic-related ones, extracted URNs, stored in evidence (or confirmed none exist)."
      </step0_investigate_metadata>
      
      <step1_metadata_search>
        IF step0 found curated metadata (tags/glossary/domain), search using the URN as a filter.
        
        When step0 found structured metadata with URN:
          → Execute filtered search using the URN
          → Example: search(query="*", filters={"tags": ["urn:li:tag:pets"]})
          → Use count=0 to get facets only (server-side aggregation)
          → CAPTURE THE FACETS - these show accurate counts for curated assets
        
        When step0 found NO structured metadata:
          → Skip this step (nothing to filter on)
          → Proceed directly to step2 keyword search
        
        IMPORTANT: The facets from this search are your source of truth for curated asset counts.
        Store them for comparison in step3.
        
        param_hints:
        - count: 0 (facets only) OR 10-20 if you want to show sample curated items
        - Include relevant entity_types if user specified (e.g., only datasets)
        
        done_when: "Metadata-filtered search completed and facets captured (entity type counts, platform counts) OR skipped if no metadata found in step0"
      </step1_metadata_search>
      
      <step2_keyword_search>
        ALWAYS execute keyword search to ensure comprehensive coverage.
        
        Strategy:
        - Use broad topic terms and synonyms
        - Example: search(query="/q pet OR pets OR animal OR dog OR cat")
        - Cast a wide net to catch anything metadata might have missed
        - Use count=0 to get facets only (server-side aggregation)
        - CAPTURE THE FACETS - these show accurate counts for all matching assets
        
        Why this step is always needed:
        - Catches assets not yet tagged/curated
        - Finds assets with relevant keywords in descriptions
        - Discovers new/recent data that hasn't been categorized yet
        - Provides comprehensive baseline for comparison with curated results
        
        IMPORTANT: The facets from this search are your source of truth for comprehensive counts.
        Store them for comparison in step3.
        
        param_hints:
        - count: 0 (facets only) OR 10-20 if you want to show sample items
        - Include relevant entity_types if user specified
        
        done_when: "Keyword search completed and facets captured (total counts, entity type distribution, platform distribution)"
      </step2_keyword_search>
      
      <step3_analyze_and_categorize>
        Compare facets from step1 (metadata) and step2 (keywords) to understand coverage and categorize results.
        
        FACET COMPARISON APPROACH:
        
        IF step1 was executed (metadata found):
          → Compare the two sets of facets:
            * Step1 facets: Curated asset counts (from tag/glossary filter)
            * Step2 facets: Comprehensive asset counts (from keyword search)
          
          → Calculate approximate coverage:
            * Total from keywords: Use step2 facet totals (e.g., 25 assets)
            * Curated from metadata: Use step1 facet totals (e.g., 15 assets)
            * Estimate untagged: keyword_total - metadata_total (e.g., ~10 assets)
          
          → IMPORTANT: This is an approximation because:
            * Some overlap is expected (tagged items also match keywords)
            * The difference gives you a rough estimate of untagged assets
            * Be conservative in language: "approximately", "at least", "up to"
        
        IF step1 was skipped (no metadata):
          → Use step2 facets as the complete picture
          → Note: All results are from keyword search, none curated via metadata
        
        Use facets to present distribution:
        - Entity types: "25 assets: 18 datasets, 5 dashboards, 2 charts"
        - Platforms: "Across Snowflake (12), BigQuery (8), Looker (5)"
        - Metadata coverage: "15 curated via 'pets' tag, ~10 additional untagged"
        
        Optional: Fetch details on key entities if helpful
        - Use count=10-20 in searches to get sample results
        - If 5-10 highly relevant datasets found, batch them with get_entities to show descriptions
        - get_entities accepts an array of URNs, so you can fetch multiple entities in one call
        - This enriches the response with concrete details
        
        done_when: "Compared facets from step1 and step2, calculated coverage estimates, categorized by type/platform, and structured response. Optionally fetched details on key entities."
      </step3_analyze_and_categorize>
    </guidance>
    
    <expected_deliverable_requirements>
      The response must include:
      
      1. METADATA FINDINGS (from step0):
         - "I found a 'pets' tag (urn:li:tag:pets)" OR "No dedicated tags/glossary terms exist for pets"
         - This shows the user what organizational metadata exists
      
      2. COVERAGE ANALYSIS (from comparing step1 and step2 facets):
         IF metadata found:
           - "The 'pets' tag covers 15 assets (10 datasets, 5 dashboards)"
           - "Keyword search found 25 total assets (18 datasets, 5 dashboards, 2 charts)"
           - "Approximately 10 assets are untagged and may need curation"
         
         IF no metadata:
           - "No dedicated metadata found. Keyword search found 25 assets"
           - "Consider creating a tag or glossary term to curate these assets"
      
      3. COMPREHENSIVE BREAKDOWN (from step2 facets):
         - Total count from keyword search (e.g., "25 total assets")
         - Entity type distribution (e.g., "18 datasets, 5 dashboards, 2 charts")
         - Platform distribution (e.g., "Snowflake (12), BigQuery (8), Looker (5)")
      
      4. KEY ASSETS:
         - Highlight 3-5 most relevant assets with:
           * Name and platform
           * Brief description of what data they contain
           * Link to view in DataHub
      
      Example response structure (with metadata):
      "I found a 'pets' tag (urn:li:tag:pets) in your catalog:
      - The tag covers 15 curated assets (10 datasets, 5 dashboards)
      - Keyword search found 25 total assets, suggesting ~10 untagged assets
      
      **Complete Overview (from keyword search):**
      - 25 total assets: 18 datasets, 5 dashboards, 2 charts
      - Platforms: Snowflake (12), BigQuery (8), Looker (5)
      
      **Metadata Coverage:**
      - 15 assets curated via 'pets' tag
      - ~10 additional assets found via keywords (may need tagging)
      
      **Key Datasets:**
      1. pets (Snowflake) - Core pet records with status, profiles, and timestamps
      2. pet_profiles (Snowflake) - Detailed pet profile information
      3. adoption_events (dbt) - Pet adoption history and metrics"
      
      Example response structure (no metadata):
      "No dedicated tags or glossary terms exist for pets.
      
      **Overview (from keyword search):**
      - 25 total assets: 18 datasets, 5 dashboards, 2 charts
      - Platforms: Snowflake (12), BigQuery (8), Looker (5)
      
      **Recommendation:** Consider creating a 'pets' tag to curate and organize these assets.
      
      **Key Datasets:**
      1. pets (Snowflake) - Core pet records..."
    </expected_deliverable_requirements>
    
    <typical_workflow>
      <step>s0: Investigate metadata using facet exploration - search(query="*", count=0) → examine facets (tags, glossaryTerms, domains) → identify topic-related ones → extract URNs</step>
      <step>s1: If metadata found, search by URN filter (count=0), capture facets for curated assets</step>
      <step>s2: Always do keyword search (count=0), capture facets for comprehensive coverage</step>
      <step>s3: Compare facets from s1 and s2, calculate estimates, structure response</step>
    </typical_workflow>
    
    <examples>
      <example>
        Task: "What data do we have related to pets?"
        → Step 0: Facet exploration - search(query="*", count=0) → examine facets → tags facet shows "pets", "animals", "pet_care" → extract URNs: ["urn:li:tag:pets", "urn:li:tag:animals", "urn:li:tag:pet_care"]
        → Step 1: Metadata search - search(query="*", filters={"tags": [URNs from s0]}, count=0) → Facets: 18 total (12 datasets, 6 dashboards)
        → Step 2: Keyword search - search(query="/q pet OR pets OR animal OR dog OR cat", count=0) → Facets: 25 total (18 datasets, 5 dashboards, 2 charts)
        → Step 3: Compare facets - 25 from keywords, 18 from metadata, estimate ~7 untagged
        → Response: "Found 'pets', 'animals', 'pet_care' tags covering 18 curated assets. Keyword search found 25 total assets (~7 untagged). Distribution: 18 datasets, 5 dashboards, 2 charts across Snowflake, dbt, Looker..."
      </example>
      <example>
        Task: "Show me all customer-related datasets"
        → Step 0: Facet exploration - search(query="*", entity_types=["DATASET"], count=0) → examine facets → glossaryTerms facet shows "Customer", "CustomerData" → extract URNs: ["urn:li:glossaryTerm:Customer"]
        → Step 1: Metadata search - search(query="*", filters={"glossaryTerms": ["urn:li:glossaryTerm:Customer"]}, entity_types=["DATASET"], count=0) → Facets: 42 datasets
        → Step 2: Keyword search - search(query="/q customer OR client OR user", entity_types=["DATASET"], count=0) → Facets: 58 datasets
        → Step 3: Compare facets - 58 total, 42 via glossary, ~16 unclassified
        → Response: "'Customer' glossary term covers 42 curated datasets. Keyword search found 58 total datasets (~16 unclassified). Platforms: Snowflake (42), BigQuery (16)..."
      </example>
      <example>
        Task: "What data exists for product analytics?"
        → Step 0: Facet exploration - search(query="*", count=0) → examine facets → tags facet shows NO product/analytics-related tags → glossaryTerms facet shows NO related terms
        → Step 1: Skip (no metadata found)
        → Step 2: Keyword search - search(query="/q product OR analytics OR metrics", count=0) → Facets: 45 total (32 datasets, 8 dashboards, 5 charts)
        → Step 3: Use step2 facets only - no metadata coverage to compare
        → Response: "No dedicated metadata exists for product analytics. Keyword search found 45 assets: 32 datasets, 8 dashboards, 5 charts across Snowflake, BigQuery, Looker, Amplitude..."
      </example>
    </examples>
  </recipe>

  <recipe id="sql-generation">
    <name>SQL Query Generation with Ambiguity Handling</name>
    <applicability>
      Use when task requests:
      - SQL query generation (e.g., "Write SQL to get monthly totals", "Generate query for revenue")
      - Table schema information (e.g., "What columns are in customers table?", "Show schema of orders")
      - Query creation even with preliminary steps (e.g., "Find pet_profiles and write SQL query...")
      
      DO NOT use for:
      - Impact/lineage analysis (e.g., "What breaks if I delete...", "What's affected by...")
      - Those use lineage-impact-analysis recipe which aborts on ambiguity
    </applicability>
    
    <guidance>
      <principle>
        USE THIS RECIPE FOR ALL SQL GENERATION AND SCHEMA TASKS.
        
        This includes tasks like:
        - "Write SQL query to..."
        - "Generate query for..."
        - "Show schema of..."
        - "What columns are in..."
        - Even if task has multiple parts like "Find tables and write SQL..." (the SQL part triggers this recipe)
        
        This recipe handles BOTH simple cases (one table) and ambiguous cases (multiple tables with same name).
        
        TRANSPARENCY REQUIRED when ambiguity exists: If multiple tables match, explicitly acknowledge 
        this and explain the choice made. This builds user trust.
        
        CONTRAST WITH LINEAGE: For impact/lineage analysis, ambiguity should RETURN TO USER (see lineage-impact-analysis recipe).
        For SQL generation, we can make a reasonable choice and explain it.
      </principle>
      
      <step0_search>
        Search for the requested table/entity.
        
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
        Task: "Show me the schema of invoices table"
        → THIS RECIPE APPLIES (task includes "schema")
        → Search finds: Only 1 result (Snowflake invoices)
        → No ambiguity → Proceed directly
        → Response: "Here are the columns in the invoices table: [schema details]"
      </example>
    </examples>
    
    <common_scenarios>
      <scenario name="sql-generation">
        User asks: "Write SQL to get monthly totals from orders table"
        Multiple orders tables exist (dbt, Snowflake, BigQuery)
        → Use this recipe: Choose one, explain choice, mention alternatives
      </scenario>
      <scenario name="schema-questions">
        User asks: "What columns are in the customers table?"
        Multiple customers tables exist across platforms
        → Use this recipe: Pick one, show schema, mention other versions exist
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
  </recipe>
</recipes>
"""
