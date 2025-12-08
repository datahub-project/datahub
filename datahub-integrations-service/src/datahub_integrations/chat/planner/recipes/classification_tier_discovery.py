"""
Recipe: Classification and Tier Discovery

Triggered when user asks about tier/classification:
- "How many assets in gold tier?"
- "Show platinum datasets"
- "Find certified datasets"
"""

RECIPE_NAME = "recipe-classification-tier-discovery"

RECIPE_XML = """\
  <recipe name="recipe-classification-tier-discovery">
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
  </recipe>"""
