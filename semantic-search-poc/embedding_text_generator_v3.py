"""
Hybrid Text Generator - Combines factual metadata with semantic templates
"""

import re
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from flattened_property_bag import FlattenedPropertyBag


@dataclass
class HybridConfig:
    """Configuration for hybrid generation"""
    include_semantic_enrichment: bool = True
    include_technical_metadata: bool = False  # For embeddings, keep false
    max_fields_shown: int = 5
    

class HybridTextGenerator:
    """
    Hybrid approach: V2 factual approach + minimal semantic enrichment
    """
    
    def __init__(self, config: Optional[HybridConfig] = None):
        self.config = config or HybridConfig()
        
        # Semantic templates based on patterns in table names/descriptions
        # These are GENERIC and work across domains
        # Ranked semantic templates: (pattern, purpose, weight, match_type)
        # match_type: 'word' (token boundary) or 'sub' (substring)
        self.semantic_templates = [
            ('transaction', 'supporting transaction analysis and financial reporting', 100, 'word'),
            ('purchase', 'tracking purchase patterns and customer behavior', 95, 'word'),
            ('order', 'supporting order management and sales analysis', 95, 'word'),
            ('revenue', 'enabling revenue tracking and financial insights', 95, 'word'),
            ('customer', 'enabling customer analytics and segmentation', 90, 'word'),
            ('product', 'supporting product catalog and inventory management', 90, 'word'),
            ('inventory', 'supporting inventory monitoring and stock control', 88, 'word'),
            ('event', 'recording event sequences and activity streams', 85, 'word'),
            ('status', 'tracking state transitions and current status', 85, 'word'),
            ('history', 'enabling historical analysis and trend tracking', 85, 'word'),
            ('session', 'tracking user sessions and behavior', 80, 'word'),
            ('profile', 'maintaining comprehensive entity information', 75, 'word'),
            ('incremental', 'with regular updates for current data visibility', 70, 'word'),
            ('last_', 'identifying most recent activity or state', 65, 'sub'),
            ('date', 'supporting time-based analysis and reporting', 50, 'word'),
            ('metric', 'supporting analytics and KPI reporting', 40, 'word'),
            ('fact', 'storing transactional fact records', 40, 'word'),
            ('dimension', 'defining reference dimensions for analysis', 40, 'word'),
        ]
        
        # Platform mappings
        self.platform_names = {
            "urn:li:dataPlatform:snowflake": "Snowflake",
            "urn:li:dataPlatform:dbt": "dbt",
            "urn:li:dataPlatform:looker": "Looker",
            "urn:li:dataPlatform:databricks": "Databricks",
            "urn:li:dataPlatform:postgres": "PostgreSQL",
            "urn:li:dataPlatform:bigquery": "BigQuery",
            "urn:li:dataPlatform:redshift": "Redshift",
            "urn:li:dataPlatform:mysql": "MySQL",
            "urn:li:dataPlatform:kafka": "Kafka",
            "urn:li:dataPlatform:s3": "Amazon S3",
            "urn:li:dataPlatform:longtail_schemas": "Long Tail Schemas"
        }
    
    def generate(self, entity: Dict[str, Any]) -> str:
        """Generate hybrid text combining factual + semantic"""
        bag = FlattenedPropertyBag(entity)
        parts = []
        
        # 1. Core description (unchanged from V2)
        if desc := bag.get('description'):
            desc = desc.replace('\n', '. ').replace('..', '.')
            parts.append(desc)
        
        # 2. Name (simplified from V2)
        name = bag.get('name')
        if name:
            desc_text = bag.get('description', consume=False) or ''
            if name.lower() not in desc_text.lower():
                readable_name = name.replace('_', ' ')
                # Neutral fallback for single-token names without description
                is_single_token = bool(re.fullmatch(r"[A-Za-z0-9]+", name))
                name_l = name.lower()
                if is_single_token and not desc_text and name_l not in {"product"}:
                    parts.append(f"The {readable_name} dataset")
                else:
                    parts.append(f'The {readable_name} table')
        
        # 3. Purpose / semantic enrichment (one concise statement)
        # Suppress when we only have a bare single-token name and no description
        suppress_inference = False
        if name and not (bag.get('description', consume=False) or ''):
            if re.fullmatch(r"[A-Za-z0-9]+", name) and name.lower() not in {"product"}:
                suppress_inference = True

        if self.config.include_semantic_enrichment and not suppress_inference:
            purpose = self._infer_purpose(bag)
            if not purpose:
                purpose = self._add_minimal_semantic(bag)
            if purpose:
                parts.append(purpose)
        
        # 4. Fields (hybrid: roles + examples)
        all_fields = self._get_fields(bag)
        if all_fields:
            summary = self._summarize_fields(all_fields)
            examples = ', '.join(all_fields[: min(3, len(all_fields))])
            if summary and examples:
                parts.append(f"{summary}; e.g., {examples}")
            elif summary:
                parts.append(summary)
            else:
                shown = all_fields[: self.config.max_fields_shown]
                field_list = ', '.join(shown)
                parts.append(f"Contains fields: {field_list}")
        
        # 5. Key metadata (streamlined from V2)
        # Domain
        if domain := bag.find_keyvalue('customProperties', 'domain'):
            parts.append(f"Part of the {domain} domain")
        
        # Owner (output as-is if available, e.g., keep full emails with '@')
        owner_value = self._extract_owner_raw(bag)
        if owner_value:
            parts.append(f"Maintained by {owner_value}")
        
        # Critical flags (compressed)
        bc = bag.find_keyvalue('customProperties', 'business_critical')
        mm = bag.find_keyvalue('customProperties', 'model_maturity')
        is_bc = str(bc).strip().lower() in {'true', 'yes', '1'} if bc is not None else False
        is_prod = str(mm).strip().lower() in {'prod', 'production'} if mm is not None else False
        if is_bc and is_prod:
            parts.append("This is a business-critical, production-ready dataset")
        elif is_bc:
            parts.append("This is a business-critical dataset")
        elif is_prod:
            parts.append("This is a production-ready dataset")
        
        # Technical (simplified)
        tech = []
        node_type = (bag.find_keyvalue('customProperties', 'node_type') or '').strip().lower()
        mat = bag.find_keyvalue('customProperties', 'materialization')
        if mat:
            tech.append(f"{mat} materialization")
        # Only add "built with" into the tech list when another tech item exists (e.g., materialization)
        lang = bag.find_keyvalue('customProperties', 'language')
        if tech and lang:
            tech.append(f"built with {lang.upper()}")
        if tech:
            parts.append("Uses " + " and ".join(tech))
        elif lang:
            # Standalone language mention without other tech
            parts.append(f"Built with {lang.upper()}")
        
        # PII (include explicit false to match earlier stronger tokens)
        if pii := bag.find_keyvalue('customProperties', 'contains_pii'):
            val = str(pii).strip().lower()
            if val in {'true', 'yes', '1'}:
                parts.append("Contains personal information")
            elif val in {'false', 'no', '0'}:
                parts.append("Does not contain personal information")
        
        # Tags are typically noisy for embeddings; omit to reduce noise
        
        # Platform and environment (include for helpful anchors)
        if platform := bag.get('platform'):
            platform_name = self.platform_names.get(platform, platform.split(':')[-1])
            parts.append(f"Stored in {platform_name}")
        if origin := bag.get('origin'):
            if origin == 'PROD':
                parts.append("From production environment")
        
        # Glossary / related concepts (prefer human-readable from customProperties)
        concepts = self._collect_glossary_concepts(bag)
        if concepts:
            parts.append(f"Related concepts: {', '.join(concepts[:2])}")
        
        # 6. Technical metadata (only if configured)
        if self.config.include_technical_metadata:
            tech_meta = []
            remaining = bag.get_remaining()
            
            for path, value in remaining.items():
                if path.startswith('customProperties/') and '=' in value:
                    key, val = value.split('=', 1)
                    val = val.strip('"').strip("'")
                    
                    # Include select technical metadata
                    if key in ['node_type', 'catalog_type', 'dbt_unique_id', 'glossary_term']:
                        tech_meta.append(f"{key}: {val}")
            
            if tech_meta:
                parts.append(f"Additional metadata - {', '.join(tech_meta[:3])}")
        
        # Join with proper punctuation; de-duplicate repeated sentences
        deduped_parts = []
        seen = set()
        for p in parts:
            key = p.strip().lower()
            if key and key not in seen:
                deduped_parts.append(p)
                seen.add(key)
        result = '. '.join(deduped_parts)
        result = re.sub(r'\.\s*\.', '.', result)
        result = re.sub(r'\s+', ' ', result)
        
        if result and not result.endswith('.'):
            result += '.'
        
        return result.strip()
    
    def _add_minimal_semantic(self, bag: FlattenedPropertyBag) -> Optional[str]:
        """Add one relevant semantic enrichment based on ranked templates."""
        name = (bag.get('name', consume=False) or '').lower()
        desc = (bag.get('description', consume=False) or '').lower()
        combined = f" {name} {desc} "
        best = None
        best_score = -1
        for pattern, purpose, weight, mtype in self.semantic_templates:
            if mtype == 'word':
                # token boundary match
                if re.search(rf"\b{re.escape(pattern)}\b", combined):
                    if weight > best_score:
                        best = purpose
                        best_score = weight
            else:
                if pattern in combined:
                    if weight > best_score:
                        best = purpose
                        best_score = weight
        if best:
            # Avoid redundancy if exact phrase already in description
            if best.split()[0] not in desc:
                return best.capitalize()
        return None

    def _infer_purpose(self, bag: FlattenedPropertyBag) -> Optional[str]:
        """Infer purpose using stronger signals from fields and properties."""
        name = (bag.get('name', consume=False) or '').lower()
        desc = (bag.get('description', consume=False) or '').lower()
        fields = [f.lower() for f in self._get_fields(bag)]
        node_type = (bag.find_keyvalue('customProperties', 'node_type') or '').strip().lower()
        domain_text = (bag.find_keyvalue('customProperties', 'domain') or '').strip().lower()
        purposes: List[str] = []
        # Temporal / recency
        if any(tok in name for tok in ['history', 'historical']) or any(tok in fields for tok in ['as_of_date', 'event_time', 'created_at', 'updated_at']):
            purposes.append('enabling historical analysis and trend tracking')
        # Status / workflow
        if any('status' in f for f in fields) or 'status' in name:
            purposes.append('tracking state transitions and workflow analysis')
        # Customer analytics cues
        if 'customer' in name or any('mktsegment' in f for f in fields):
            purposes.append('enabling customer analytics and segmentation')
        # Sales/transaction cues
        has_name_sales_token = any(tok in name for tok in ['order', 'transaction', 'purchase', 'revenue', 'payment', 'sale'])
        has_field_amount_like = any(any(tok in f for tok in ['amount', 'price', 'revenue']) for f in fields)
        has_field_sales_token = any(any(tok in f for tok in ['order', 'purchase', 'transaction', 'payment', 'invoice']) for f in fields)
        in_commerce_domain = any(tok in domain_text for tok in ['commerce', 'sales', 'revenue'])
        # Guard against over-inference: require stronger signals than just a numeric amount-like field.
        # - Accept if: name clearly indicates sales, OR (amount-like AND sales-like fields), OR domain is commerce/sales
        # - Additionally, avoid assigning this purpose for seeds unless there is a strong name/domain signal
        if (
            has_name_sales_token
            or (has_field_amount_like and has_field_sales_token)
            or in_commerce_domain
        ) and not (node_type == 'seed' and not (has_name_sales_token or in_commerce_domain)):
            purposes.append('supporting sales and revenue analysis')
        # Keep only the strongest two distinct purposes
        dedup = []
        for p in purposes:
            if p not in dedup:
                dedup.append(p)
            if len(dedup) == 2:
                break
        if dedup:
            return (dedup[0][0].upper() + dedup[0][1:]) if len(dedup) == 1 else f"{dedup[0][0].upper() + dedup[0][1:]} and {dedup[1]}"
        return None
    
    def _get_fields(self, bag: FlattenedPropertyBag) -> List[str]:
        """Get field names"""
        all_fields = []
        
        field_paths = bag.enumerate('fieldPaths/*')
        for path, value in field_paths.items():
            if not value.startswith('[version='):
                all_fields.append(value)
        
        fields = bag.enumerate('fields/*')
        for path, value in fields.items():
            if value not in all_fields and not value.startswith('['):
                all_fields.append(value)
        
        return all_fields

    def _summarize_fields(self, fields: List[str]) -> Optional[str]:
        """Summarize fields into semantic roles instead of listing many names."""
        if not fields:
            return None
        roles: List[str] = []
        lower_fields = [f.lower() for f in fields]
        if any('id' in f and any(k in f for k in ['user', 'profile', 'customer', 'account']) for f in lower_fields):
            roles.append('identifiers')
        if any('status' in f for f in lower_fields):
            roles.append('status')
        if any(any(k in f for k in ['date', 'time', 'timestamp']) for f in lower_fields):
            roles.append('timestamps')
        if any(any(k in f for k in ['amount', 'price', 'value', 'revenue']) for f in lower_fields):
            roles.append('numeric measures')
        if any('name' in f for f in lower_fields):
            roles.append('names')
        if not roles:
            return None
        if len(roles) == 1:
            return f"Tracks {roles[0]}"
        if len(roles) == 2:
            return f"Tracks {roles[0]} and {roles[1]}"
        return f"Tracks {', '.join(roles[:-1])}, and {roles[-1]}"

    def _is_uuid_like(self, text: str) -> bool:
        """Heuristic to filter UUID-like glossary terms."""
        return bool(re.fullmatch(r"[0-9a-fA-F\-]{8,}", text))

    def _humanize_label(self, value: str) -> str:
        """Convert file-like or token strings to readable concepts."""
        val = value.strip().strip('"').strip("'")
        # Drop common file extensions
        val = re.sub(r"\.(md|rst|txt|csv|json|yaml|yml)$", "", val, flags=re.IGNORECASE)
        # Replace separators with spaces
        val = val.replace('_', ' ').replace('-', ' ').replace('.', ' ')
        # Collapse multiple spaces
        val = re.sub(r"\s+", " ", val).strip()
        # Lowercase for embeddings-friendly phrase
        return val

    def _collect_glossary_concepts(self, bag: FlattenedPropertyBag) -> List[str]:
        """Collect human-readable glossary concepts from custom properties and glossaryTerms."""
        concepts: List[str] = []
        # From customProperties entries like glossary_term=return_rate.md
        custom_props = bag.enumerate('customProperties/*')
        for path, kv in custom_props.items():
            if '=' in kv:
                key, val = kv.split('=', 1)
                key_l = key.strip().lower()
                if key_l in {'glossary_term', 'glossary', 'concept'}:
                    human = self._humanize_label(val)
                    if human and len(human) < 60:
                        concepts.append(human)
        # From glossaryTerms URNs if not UUID-like
        glossary_terms = bag.enumerate('glossaryTerms/*')
        for path, term_urn in glossary_terms.items():
            term_name = term_urn.split(':')[-1] if ':' in term_urn else term_urn
            if (not term_name.startswith('[')) and (len(term_name) < 50) and (not self._is_uuid_like(term_name)):
                human = self._humanize_label(term_name)
                if human:
                    concepts.append(human)
        # Deduplicate while preserving order
        seen = set()
        deduped: List[str] = []
        for c in concepts:
            if c not in seen:
                deduped.append(c)
                seen.add(c)
        return deduped

    def _extract_owner_raw(self, bag: FlattenedPropertyBag) -> Optional[str]:
        """Return the owner value exactly as stored.

        Preference order:
        1) customProperties.owner -> return value as-is (including '@' etc.)
        2) owners array -> return identifier segment of URN as-is
        """
        # 1) customProperties owner
        owner_prop = bag.find_keyvalue('customProperties', 'owner')
        if owner_prop:
            return owner_prop.strip()

        # 2) owners array URNs
        owners = bag.enumerate('owners/*', consume=False)
        for _, urn in sorted(owners.items()):
            if urn.startswith('urn:li:corpuser:') or urn.startswith('urn:li:corpGroup:'):
                ident = urn.split(':')[-1]
                return ident.strip()
        return None

    def _extract_owner_label(self, bag: FlattenedPropertyBag) -> Optional[str]:
        """Prefer human-readable owner from owners array, fallback to customProperties.owner.

        Returns a title-cased concise label like "Shannon" or "Bi Engineering". Returns None if not derivable.
        """
        # Try owners array first (URNs like urn:li:corpuser:alice@example.com or urn:li:corpGroup:bi-engineering)
        owners = bag.enumerate('owners/*', consume=False)
        for _, urn in sorted(owners.items()):
            if urn.startswith('urn:li:corpuser:'):
                ident = urn.split(':')[-1]
                # If email, take local-part; else take identifier
                local = ident.split('@')[0]
                label = local.replace('_', ' ').replace('-', ' ').title().strip()
                if label:
                    return label
            elif urn.startswith('urn:li:corpGroup:'):
                ident = urn.split(':')[-1]
                label = ident.replace('_', ' ').replace('-', ' ').title().strip()
                if label:
                    return label
        # Fallback to customProperties owner
        owner_prop = bag.find_keyvalue('customProperties', 'owner')
        if not owner_prop:
            return None
        owner_prop = owner_prop.strip()
        # Strip leading '@' (Slack-style handles)
        if owner_prop.startswith('@'):
            owner_prop = owner_prop[1:]
        # If email, take local-part; otherwise, use as identifier
        if '@' in owner_prop and owner_prop.find('@') > 0:
            owner_core = owner_prop.split('@')[0]
        else:
            owner_core = owner_prop
        label = owner_core.replace('_', ' ').replace('-', ' ').title().strip()
        return label or None


# Presets for different use cases
def create_embedding_generator() -> HybridTextGenerator:
    """Optimized for embeddings - minimal, semantic"""
    return HybridTextGenerator(HybridConfig(
        include_semantic_enrichment=True,
        include_technical_metadata=False,
        max_fields_shown=3
    ))


def create_documentation_generator() -> HybridTextGenerator:
    """For documentation - include everything"""
    return HybridTextGenerator(HybridConfig(
        include_semantic_enrichment=True,
        include_technical_metadata=True,
        max_fields_shown=10
    ))


# Main V3 generator class - no config needed
class EmbeddingTextGeneratorV3(HybridTextGenerator):
    """V3 Generator using hybrid approach with semantic enrichment"""
    
    def __init__(self, config=None):
        # Ignore any config passed, always use optimal settings
        optimal_config = HybridConfig(
            include_semantic_enrichment=True,
            include_technical_metadata=False,
            max_fields_shown=5
        )
        super().__init__(optimal_config)
    
    def generate(self, entity: Dict[str, Any], include_unknown: Optional[bool] = None) -> str:
        """Generate text - ignores include_unknown for simplicity"""
        return super().generate(entity)


# Backward compatibility - these are ignored but kept so old code doesn't break
from enum import Enum

class TextStyle(Enum):
    """Backward compatibility only - not actually used"""
    NATURAL = "natural"
    CONCISE = "concise"
    TECHNICAL = "technical"
    SEMANTIC = "semantic"


@dataclass
class TextGeneratorConfig:
    """Backward compatibility only - ignored by V3"""
    style: TextStyle = TextStyle.NATURAL
    include_all_metadata: bool = False
    include_ids: bool = False
    max_fields_shown: int = 5


# Backward compatibility alias
CleanTextConfig = TextGeneratorConfig


# Example usage
if __name__ == "__main__":
    sample_data = {
        "name": "pet_status_history",
        "description": "Incremental table containing all historical statuses of a pet",
        "platform": "urn:li:dataPlatform:dbt",
        "origin": "PROD",
        "tags": ["business_critical", "prod_model"],
        "customProperties": [
            "owner=shannon@longtail.com",
            "materialization=incremental",
            "business_critical=True",
            "language=sql",
            "contains_pii=False",
            "domain=Pet Adoptions",
            "model_maturity=prod",
            "account_id=107298",
            "project_id=241624",
            "job_id=279117",
            "catalog_type=BASE TABLE",
            "dbt_unique_id=model.long_tail_companions.pet_status_history",
        ],
        "fieldPaths": ["profile_id", "status", "as_of_date"],
    }
    
    generator = create_embedding_generator()
    print("Hybrid Approach (Embedding-optimized):")
    print("-" * 50)
    print(generator.generate(sample_data))