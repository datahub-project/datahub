"""
PipeConcatenator - Simple pipe concatenation approach for comparison
"""
from typing import Dict, List, Any


class PipeConcatenator:
    """Simple pipe concatenation approach for comparison"""
    
    def generate(self, entity: Dict[str, Any]) -> str:
        """Generate pipe-separated concatenation of entity fields"""
        parts = []
        
        # Add name
        if entity.get('name'):
            parts.append(entity['name'])
        
        # Add description
        if entity.get('description'):
            # Replace newlines with spaces in description
            desc = entity['description'].replace('\n', ' ')
            parts.append(desc)
        
        # Add qualified name
        if entity.get('qualifiedName'):
            parts.append(entity['qualifiedName'])
        
        # Add tags
        if entity.get('tags'):
            parts.extend(entity['tags'])
        
        # Add glossary terms
        if entity.get('glossaryTerms'):
            parts.extend(entity['glossaryTerms'])
        
        # Add custom properties
        if entity.get('customProperties'):
            parts.extend(entity['customProperties'])
        
        # Add ALL field paths (no truncation)
        if entity.get('fieldPaths'):
            parts.extend(entity['fieldPaths'])
        
        # Add platform
        if entity.get('platform'):
            parts.append(entity['platform'])
        
        # Add origin
        if entity.get('origin'):
            parts.append(entity['origin'])
        
        # Join with pipe separator
        return ' | '.join(parts)
