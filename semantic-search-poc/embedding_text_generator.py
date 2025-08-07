"""
EmbeddingTextGenerator - Creates natural language descriptions for DataHub entities
"""
from typing import Dict, List, Any
import re


class EmbeddingTextGenerator:
    """Generates natural language descriptions optimized for embeddings"""
    
    def __init__(self):
        self.platform_names = {
            "urn:li:dataPlatform:snowflake": "Snowflake",
            "urn:li:dataPlatform:dbt": "dbt",
            "urn:li:dataPlatform:looker": "Looker",
            "urn:li:dataPlatform:databricks": "Databricks",
            "urn:li:dataPlatform:postgres": "PostgreSQL",
            "urn:li:dataPlatform:longtail_schemas": "Long Tail Schemas"
        }
    
    def generate(self, entity: Dict[str, Any]) -> str:
        """Generate natural language description for an entity"""
        parts = []
        
        # Start with the actual description if available
        if entity.get('description'):
            # Clean up multi-line descriptions
            desc = entity['description'].replace('\n', '. ').replace('..', '.')
            parts.append(desc)
        
        # Add entity name context
        if entity.get('name'):
            readable_name = self._humanize_name(entity['name'])
            
            # Only add name context if no description or description doesn't mention the name
            if not entity.get('description'):
                parts.append(f"This is the {readable_name} dataset")
            elif readable_name.lower() not in entity.get('description', '').lower():
                parts.append(f"The {readable_name} table")
        
        # Add location/qualified name in natural way
        if entity.get('qualifiedName') and entity.get('qualifiedName') != entity.get('name'):
            parts.append(f"Located at {entity['qualifiedName']}")
        
        # Add schema/field information naturally
        if entity.get('fieldPaths'):
            fields = entity['fieldPaths']
            # Filter out complex protobuf-style fields
            simple_fields = [f for f in fields if not f.startswith('[version=')]
            
            if simple_fields:
                if len(simple_fields) <= 5:
                    field_list = ', '.join(simple_fields)
                    parts.append(f"Contains fields: {field_list}")
                else:
                    field_list = ', '.join(simple_fields[:3])
                    parts.append(f"Contains {len(simple_fields)} fields including {field_list}")
        
        # Add business context from custom properties
        if entity.get('customProperties'):
            custom_props = self._parse_custom_properties(entity['customProperties'])
            
            # Domain information
            if 'domain' in custom_props:
                parts.append(f"Part of the {custom_props['domain']} domain")
            
            # Ownership
            if 'owner' in custom_props:
                owner = custom_props['owner'].replace('@longtail.com', '')
                parts.append(f"Maintained by {owner}")
            
            # Data quality/maturity
            if 'model_maturity' in custom_props:
                maturity = custom_props['model_maturity']
                if maturity == 'prod':
                    parts.append("This is a production-ready model")
            
            # Technical details
            tech_details = []
            if 'materialization' in custom_props:
                tech_details.append(f"{custom_props['materialization']} materialization")
            if 'language' in custom_props:
                tech_details.append(f"built with {custom_props['language'].upper()}")
            if tech_details:
                parts.append(f"Uses {' and '.join(tech_details)}")
        
        # Add tags in a natural way
        if entity.get('tags'):
            tag_names = [self._extract_tag_name(t) for t in entity['tags']]
            readable_tags = [self._humanize_name(t) for t in tag_names]
            
            if 'business_critical' in tag_names:
                parts.append("This is a business-critical dataset")
            elif readable_tags:
                parts.append(f"Tagged as: {', '.join(readable_tags)}")
        
        # Add platform information
        if entity.get('platform'):
            platform = self.platform_names.get(entity['platform'], entity['platform'])
            if platform and platform not in ' '.join(parts):
                parts.append(f"Stored in {platform}")
        
        # Add environment
        if entity.get('origin'):
            origin_map = {'PROD': 'production', 'DEV': 'development', 'TEST': 'test'}
            env = origin_map.get(entity['origin'], entity['origin'].lower())
            if env and env not in ' '.join(parts).lower():
                parts.append(f"From {env} environment")
        
        # Join with proper punctuation
        result = '. '.join(parts)
        result = re.sub(r'\.\s*\.', '.', result)  # Remove double periods
        result = re.sub(r'\s+', ' ', result)  # Normalize whitespace
        
        return result.strip()
    
    def _humanize_name(self, name: str) -> str:
        """Convert technical names to readable format"""
        # Handle UPPERCASE names
        if name.isupper():
            return name.lower()
        
        # Convert snake_case and kebab-case to spaces
        name = name.replace('_', ' ').replace('-', ' ')
        
        # Handle camelCase
        name = re.sub(r'([a-z])([A-Z])', r'\1 \2', name)
        
        return name.lower().strip()
    
    def _extract_tag_name(self, tag_urn: str) -> str:
        """Extract tag name from URN"""
        if ':' in tag_urn:
            return tag_urn.split(':')[-1]
        return tag_urn
    
    def _parse_custom_properties(self, props: List[str]) -> Dict[str, str]:
        """Parse custom properties list into dictionary"""
        result = {}
        for prop in props:
            if '=' in prop:
                key, value = prop.split('=', 1)
                result[key] = value.strip('"').strip("'")
        return result
