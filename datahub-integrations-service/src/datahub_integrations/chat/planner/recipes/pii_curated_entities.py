"""
Recipe: PII Discovery - Requires Explicit Tagging

Triggered when user asks about PII/sensitive data entities:
- "Show datasets containing PII"
- "Find tables with personal information"
- "Find jobs that access PII"
"""

RECIPE_NAME = "recipe-pii-curated-entities"

RECIPE_XML = """\
  <recipe name="recipe-pii-curated-entities">
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
  </recipe>"""
