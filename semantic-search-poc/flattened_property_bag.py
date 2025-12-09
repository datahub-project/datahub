"""
FlattenedPropertyBag - Flattens nested structures into a single-level map with path keys
"""
from typing import Mapping, Dict, Any, Optional, Set
from collections.abc import Mapping as MappingABC


class FlattenedPropertyBag:
    """
    Flattens nested structures into a single-level immutable map with path keys.
    Tracks property consumption for identifying unused fields.
    """
    
    def __init__(self, data: Mapping[str, Any], separator: str = '/'):
        """
        Initialize with a mapping (dict-like object).
        
        Args:
            data: The nested data structure to flatten
            separator: Path separator (default: '/')
        """
        self._separator = separator
        self._consumed: Set[str] = set()
        
        # Flatten the input data into an immutable map
        flattened = self._flatten(data)
        self._properties: Mapping[str, str] = flattened
        
        # Store original for reference
        self._original: Mapping[str, Any] = data
    
    def _flatten(self, obj: Any, prefix: str = '') -> Dict[str, str]:
        """
        Recursively flatten nested structures into path/value pairs.
        
        Args:
            obj: The object to flatten
            prefix: The current path prefix
            
        Returns:
            Dict with flattened paths as keys and string values
        """
        result = {}
        
        if isinstance(obj, MappingABC):
            # Handle dict-like objects
            for key, value in obj.items():
                new_key = f"{prefix}{self._separator}{key}" if prefix else key
                result.update(self._flatten(value, new_key))
                
        elif isinstance(obj, (list, tuple)):
            # Handle sequences with numeric indices
            for i, item in enumerate(obj):
                new_key = f"{prefix}{self._separator}{i}" if prefix else str(i)
                if isinstance(item, (MappingABC, list, tuple)):
                    result.update(self._flatten(item, new_key))
                else:
                    result[new_key] = str(item) if item is not None else ''
                    
        else:
            # Leaf value - convert to string
            result[prefix] = str(obj) if obj is not None else ''
            
        return result
    
    def get(self, path: str, default: Optional[str] = None, consume: bool = True) -> Optional[str]:
        """
        Get a single property by exact path.
        
        Args:
            path: The path to the property
            default: Default value if not found
            consume: Whether to mark this property as consumed
            
        Returns:
            The property value or default
        """
        value = self._properties.get(path, default)
        if value is not None and consume:
            self._consumed.add(path)
        return value
    
    def enumerate(self, pattern: str, consume: bool = True) -> Dict[str, str]:
        """
        Enumerate all properties matching a pattern.
        
        Patterns:
            - 'customProperties/*' - all direct children
            - 'customProperties/**' - all descendants (recursive)
            - 'tags/*/name' - wildcards in the middle
            - 'exact/path' - exact match
            
        Args:
            pattern: The pattern to match
            consume: Whether to mark matched properties as consumed
            
        Returns:
            Dict of matching paths and their values
        """
        results = {}
        
        if pattern.endswith('/*'):
            # Match direct children only
            prefix = pattern[:-2]
            prefix_with_sep = f"{prefix}{self._separator}" if prefix else ""
            
            for key, value in self._properties.items():
                if prefix:
                    if not key.startswith(prefix_with_sep):
                        continue
                    relative = key[len(prefix_with_sep):]
                else:
                    relative = key
                    
                # Check if it's a direct child (no more separators)
                if self._separator not in relative:
                    results[key] = value
                    if consume:
                        self._consumed.add(key)
                        
        elif pattern.endswith('/**'):
            # Match all descendants recursively
            prefix = pattern[:-3]
            prefix_with_sep = f"{prefix}{self._separator}" if prefix else ""
            
            for key, value in self._properties.items():
                if not prefix or key.startswith(prefix_with_sep):
                    results[key] = value
                    if consume:
                        self._consumed.add(key)
                        
        elif '*' in pattern:
            # Wildcard matching - simple implementation
            # Convert pattern to regex-like matching
            import re
            regex_pattern = pattern.replace('*', '[^' + self._separator + ']*')
            regex_pattern = f"^{regex_pattern}$"
            compiled = re.compile(regex_pattern)
            
            for key, value in self._properties.items():
                if compiled.match(key):
                    results[key] = value
                    if consume:
                        self._consumed.add(key)
        else:
            # Exact match
            if value := self.get(pattern, consume=consume):
                results[pattern] = value
                
        return results
    
    def exists(self, pattern: str) -> bool:
        """
        Check if any properties match the pattern.
        
        Args:
            pattern: The pattern to check
            
        Returns:
            True if at least one property matches
        """
        return bool(self.enumerate(pattern, consume=False))
    
    def find_in_array(self, array_prefix: str, search_fn, consume: bool = True) -> Optional[str]:
        """
        Find first item in array matching a condition.
        
        Args:
            array_prefix: Path to the array
            search_fn: Function that returns True for matching items
            consume: Whether to mark found item as consumed
            
        Returns:
            The first matching value or None
        """
        items = self.enumerate(f"{array_prefix}/*", consume=False)
        for path, value in sorted(items.items()):  # Sort to ensure consistent ordering
            if search_fn(value):
                if consume:
                    self._consumed.add(path)
                return value
        return None
    
    def find_keyvalue(self, array_prefix: str, key: str, consume: bool = True) -> Optional[str]:
        """
        Find key=value pair in array and return the value.
        
        Args:
            array_prefix: Path to array containing key=value strings
            key: The key to search for
            consume: Whether to mark as consumed
            
        Returns:
            The value part of the key=value pair, or None
        """
        value = self.find_in_array(
            array_prefix,
            lambda v: v.startswith(f"{key}="),
            consume=consume
        )
        if value:
            return value.split('=', 1)[1].strip('"').strip("'")
        return None
    
    def get_remaining(self) -> Dict[str, str]:
        """
        Get all unconsumed properties.
        
        Returns:
            Dict of unconsumed paths and values
        """
        return {
            key: value 
            for key, value in self._properties.items() 
            if key not in self._consumed
        }
    
    def _get_consumed(self) -> Set[str]:
        """
        Internal: Get set of consumed property paths.
        
        Returns:
            Set of consumed paths
        """
        return self._consumed.copy()
    
    def _get_all_properties(self) -> Mapping[str, str]:
        """
        Internal: Get all flattened properties (consumed and unconsumed).
        
        Returns:
            All properties as an immutable mapping
        """
        return self._properties
    
    def __repr__(self) -> str:
        total = len(self._properties)
        consumed = len(self._consumed)
        return f"FlattenedPropertyBag(total={total}, consumed={consumed}, remaining={total-consumed})"
