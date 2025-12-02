"""
Recipe: Broad Data Discovery with Metadata Investigation

Triggered when user asks about finding data by topic:
- "What data do we have about pets?"
- "Show me customer-related assets"
- "Find all marketing data"
"""

RECIPE_ID = "broad-data-discovery"

RECIPE_XML = """\
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
        - OPTION A (for facets + sample results): PREFER smart_search when available - it provides
          AI-powered relevance ranking and returns DETAILED entity info:
          smart_search(
            semantic_query="find all data related to {topic}",
            keyword_search_query="/q {topic_keywords} OR {synonyms}",
            filters={"entity_type": [...]} if specified
          )
          Note: smart_search returns facets AND detailed results, but doesn't support count=0
        - OPTION B (for facets only with count=0): Use regular search:
          search(query="/q pet OR pets OR animal OR dog OR cat", count=0)
        - Cast a wide net to catch anything metadata might have missed
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
  </recipe>"""
