"""
EmbeddingTextGenerator V2 - Using FlattenedPropertyBag for cleaner property extraction
"""
from typing import Dict, List, Any
import re
from flattened_property_bag import FlattenedPropertyBag


class EmbeddingTextGeneratorV2:
    """Generates natural language descriptions optimized for embeddings using PropertyBag"""
    
    def __init__(self):
        self.platform_names = {
            "urn:li:dataPlatform:snowflake": "Snowflake",
            "urn:li:dataPlatform:dbt": "dbt",
            "urn:li:dataPlatform:looker": "Looker",
            "urn:li:dataPlatform:databricks": "Databricks",
            "urn:li:dataPlatform:postgres": "PostgreSQL",
            "urn:li:dataPlatform:longtail_schemas": "Long Tail Schemas"
        }
    
    def generate(self, entity: Dict[str, Any], include_unknown: bool = False) -> str:
        """Generate natural language description for an entity"""
        bag = FlattenedPropertyBag(entity)
        parts = []
        
        # Start with the actual description if available
        if desc := bag.get('description'):
            # Clean up multi-line descriptions
            desc = desc.replace('\n', '. ').replace('..', '.')
            parts.append(desc)
        
        # Add entity name context
        if name := bag.get('name'):
            # Keep original name for technical accuracy
            # Only add name context if no description or description doesn't mention the name
            desc_text = bag.get('description', consume=False) or ''
            if not desc_text:
                parts.append(f'This is the "{name}" dataset')
            elif name.lower() not in desc_text.lower():
                parts.append(f'The "{name}" table')
        
        # Add location/qualified name in natural way
        if qualified := bag.get('qualifiedName'):
            name_value = bag.get('name', consume=False)
            if qualified != name_value:
                parts.append(f"Located at {qualified}")
        
        # Add schema/field information naturally
        field_paths = bag.enumerate('fieldPaths/*')
        if field_paths:
            # Filter out complex protobuf-style fields
            simple_fields = []
            for path, value in field_paths.items():
                if not value.startswith('[version='):
                    simple_fields.append(value)
            
            if simple_fields:
                if len(simple_fields) <= 5:
                    # Quote field names for clarity
                    field_list = ', '.join([f'"{f}"' for f in simple_fields])
                    parts.append(f"Contains fields: {field_list}")
                else:
                    # Show first 3 with quotes
                    field_list = ', '.join([f'"{f}"' for f in simple_fields[:3]])
                    parts.append(f"Contains {len(simple_fields)} fields including {field_list}")
        
        # Add business context from custom properties
        # Domain information
        if domain := bag.find_keyvalue('customProperties', 'domain'):
            parts.append(f"Part of the {domain} domain")
        
        # Ownership
        if owner := bag.find_keyvalue('customProperties', 'owner'):
            parts.append(f"Maintained by {owner}")
        
        # Data quality/maturity
        if maturity := bag.find_keyvalue('customProperties', 'model_maturity'):
            if maturity == 'prod':
                parts.append("This is a production-ready model")
        
        # Business critical flag
        if bag.find_keyvalue('customProperties', 'business_critical') == 'True':
            parts.append("This is a business-critical dataset")
        
        # Technical details
        tech_details = []
        if materialization := bag.find_keyvalue('customProperties', 'materialization'):
            tech_details.append(f"{materialization} materialization")
        if language := bag.find_keyvalue('customProperties', 'language'):
            tech_details.append(f"built with {language.upper()}")
        if tech_details:
            parts.append(f"Uses {' and '.join(tech_details)}")
        
        # Add glossary terms if present
        glossary_terms = bag.enumerate('glossaryTerms/*')
        if glossary_terms:
            glossary_names = []
            for path, term_urn in glossary_terms.items():
                # Extract the last part of the URN or use as-is
                if ':' in term_urn:
                    # For URNs like urn:li:glossaryTerm:9afa9a59-93b2-47cb-9094-aa342eec24ad
                    # We might want to keep the ID or look for a name
                    glossary_names.append(term_urn.split(':')[-1])
                else:
                    glossary_names.append(term_urn)
            
            if glossary_names:
                parts.append(f"Glossary terms: {', '.join(glossary_names)}")
        
        # Add tags in a natural way
        tags = bag.enumerate('tags/*')
        if tags:
            # Check for specific important tags first
            has_business_critical = False
            tag_names = []
            
            for path, tag_urn in tags.items():
                if ':' in tag_urn:
                    tag_name = tag_urn.split(':')[-1]
                else:
                    tag_name = tag_urn
                
                if tag_name == 'business_critical':
                    has_business_critical = True
                else:
                    # Keep original tag names for accuracy
                    tag_names.append(f'"{tag_name}"')
            
            # Only mention business critical if not already mentioned via customProperties
            if has_business_critical and 'business-critical' not in ' '.join(parts):
                parts.append("This is a business-critical dataset")
            
            if tag_names:
                parts.append(f"Tagged as: {', '.join(tag_names)}")
        
        # Add platform information
        if platform := bag.get('platform'):
            platform_name = self.platform_names.get(platform, platform)
            # Check if platform not already mentioned
            if platform_name and platform_name.lower() not in ' '.join(parts).lower():
                parts.append(f"Stored in {platform_name}")
        
        # Add environment
        if origin := bag.get('origin'):
            origin_map = {'PROD': 'production', 'DEV': 'development', 'TEST': 'test'}
            env = origin_map.get(origin, origin.lower())
            if env and env not in ' '.join(parts).lower():
                parts.append(f"From {env} environment")
        
        # Handle remaining properties if requested
        if include_unknown:
            remaining = bag.get_remaining()
            
            # Log what we didn't handle
            if remaining:
                # Group by top-level keys
                top_level_remaining = {}
                for path in remaining:
                    top_key = path.split('/')[0]
                    if top_key not in ['customProperties', 'tags', 'fieldPaths']:  # Skip arrays we've processed
                        if top_key not in top_level_remaining:
                            top_level_remaining[top_key] = []
                        top_level_remaining[top_key].append(path)
                
                # Also check for unconsumed custom properties
                remaining_custom = []
                for path, value in remaining.items():
                    if path.startswith('customProperties/'):
                        # Parse the key=value from the value
                        if '=' in value:
                            key, val = value.split('=', 1)
                            val = val.strip('"').strip("'")
                            # Include ALL properties now (including IDs)
                            # Quote string values, leave booleans and numbers as-is
                            if val.lower() in ['true', 'false'] or val.replace('.', '').isdigit():
                                remaining_custom.append(f"{key}: {val}")
                            else:
                                remaining_custom.append(f'{key}: "{val}"')
                
                if remaining_custom:
                    # Include ALL remaining metadata, not just first 3
                    parts.append(f"Additional metadata - {', '.join(remaining_custom)}")
                
                # Debug logging (commented out for production)
                # print(f"[DEBUG] Unconsumed paths: {list(remaining.keys())[:10]}")
        
        # Join with proper punctuation
        result = '. '.join(parts)
        result = re.sub(r'\.\s*\.', '.', result)  # Remove double periods
        result = re.sub(r'\s+', ' ', result)  # Normalize whitespace
        
        return result.strip()
    

