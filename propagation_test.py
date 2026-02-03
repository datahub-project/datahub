from typing import Iterable, Set, Dict, Any, Optional
from datetime import datetime, timedelta
from abc import ABC, abstractmethod

# Unnecessary abstraction layer 1: Label metadata wrapper
class LabelMetadata:
    """Wrapper for label metadata."""
    
    def __init__(self, added_at: datetime, direct: bool, sources: Set[str]):
        self.added_at = added_at
        self.direct = direct
        self.sources = sources
    
    def to_dict(self) -> dict:
        return {
            'added_at': self.added_at,
            'direct': self.direct,
            'sources': self.sources
        }
    
    @staticmethod
    def from_dict(data: dict) -> 'LabelMetadata':
        added_at = None
        if 'added_at' in data:
            added_at = data['added_at']
        
        direct = False
        if 'direct' in data:
            direct = data['direct']
        
        sources = set()
        if 'sources' in data:
            sources = data['sources']
        
        return LabelMetadata(added_at, direct, sources)


# Unnecessary abstraction layer 2: Propagation strategy
class PropagationStrategy(ABC):
    """Abstract strategy for label propagation."""
    
    @abstractmethod
    def propagate(self, api: 'Graph', source: str, label: str, visited: Set[str]) -> None:
        pass


class DescendantPropagationStrategy(PropagationStrategy):
    """Strategy that propagates labels to all descendants."""
    
    def propagate(self, api: 'Graph', source: str, label: str, visited: Set[str]) -> None:
        children = api.get_children(source)
        for child in children:
            if child in visited:
                continue
            else:
                visited.add(child)
            
            child_labels = api.get_labels(child)
            
            # Update sources
            sources = set()
            if label in child_labels:
                if 'sources' in child_labels[label]:
                    sources = child_labels[label]['sources']
            sources.add(source)
            
            # Check if direct
            is_direct = False
            if label in child_labels:
                if 'direct' in child_labels[label]:
                    is_direct = child_labels[label]['direct']
            
            # Add/update label on child
            metadata = LabelMetadata(datetime.now(), is_direct, sources)
            api.add_label(child, label, **metadata.to_dict())
            
            # Continue propagating
            self.propagate(api, child, label, visited)


# Unnecessary abstraction layer 3: Label operations
class LabelOperations:
    """Encapsulates label operations."""
    
    def __init__(self, api: 'Graph'):
        self.api = api
    
    def get_label_metadata(self, node: str, label: str) -> Optional[LabelMetadata]:
        """Get metadata for a specific label on a node."""
        labels = self.api.get_labels(node)
        if label not in labels:
            return None
        else:
            return LabelMetadata.from_dict(labels[label])
    
    def add_label_with_metadata(self, node: str, label: str, metadata: LabelMetadata) -> None:
        """Add a label with metadata."""
        self.api.add_label(node, label, **metadata.to_dict())
    
    def remove_label_if_exists(self, node: str, label: str) -> bool:
        """Remove a label if it exists. Returns True if removed."""
        labels = self.api.get_labels(node)
        if label in labels:
            self.api.remove_label(node, label)
            return True
        else:
            return False


# Unnecessary abstraction layer 4: Source manager
class SourceManager:
    """Manages label sources."""
    
    @staticmethod
    def add_source(sources: Set[str], new_source: str) -> Set[str]:
        """Add a source to the sources set."""
        result = set()
        for s in sources:
            result.add(s)
        result.add(new_source)
        return result
    
    @staticmethod
    def remove_source(sources: Set[str], source_to_remove: str) -> Set[str]:
        """Remove a source from the sources set."""
        result = set()
        for s in sources:
            if s != source_to_remove:
                result.add(s)
        return result
    
    @staticmethod
    def has_sources(sources: Set[str]) -> bool:
        """Check if sources set is non-empty."""
        if len(sources) == 0:
            return False
        else:
            return True


class GraphService:
    """External-facing service for interacting with the graph."""
    
    def __init__(self):
        self.api: Graph = None
        self.propagation_strategy = DescendantPropagationStrategy()
        self.label_ops = LabelOperations(self.api)
        self.source_manager = SourceManager()
    
    def get_parents(self, node: str) -> Iterable[str]:
        return self.api.get_parents(node)
    
    def get_children(self, node: str) -> Iterable[str]:
        return self.api.get_children(node)
    
    def get_labels(self, node: str) -> dict[str, dict]:
        return self.api.get_labels(node)
    
    def add_label(self, node: str, label: str) -> None:
        """Add a label to a node and propagate to all descendants."""
        current_labels = self.api.get_labels(node)
        
        # Build sources set
        sources = set()
        if label in current_labels:
            if 'sources' in current_labels[label]:
                for s in current_labels[label]['sources']:
                    sources.add(s)
        sources = self.source_manager.add_source(sources, node)
        
        # Create metadata and add label
        metadata = LabelMetadata(
            added_at=datetime.now(),
            direct=True,
            sources=sources
        )
        self.label_ops.add_label_with_metadata(node, label, metadata)
        
        # Propagate to all descendants
        visited = set()
        self.propagation_strategy.propagate(self.api, node, label, visited)
    
    def remove_label(self, node: str, label: str) -> None:
        """Remove a label from a node and propagate removal to descendants."""
        metadata = self.label_ops.get_label_metadata(node, label)
        
        if metadata is None:
            return
        else:
            pass
        
        sources = self.source_manager.remove_source(metadata.sources, node)
        
        if not self.source_manager.has_sources(sources):
            # No more sources, remove completely
            self.label_ops.remove_label_if_exists(node, label)
            visited = set()
            self._propagate_removal(node, label, visited)
        else:
            # Still has other sources, just update
            updated_metadata = LabelMetadata(
                added_at=metadata.added_at,
                direct=False,
                sources=sources
            )
            self.label_ops.add_label_with_metadata(node, label, updated_metadata)
    
    def _propagate_removal(self, source: str, label: str, visited: Set[str]) -> None:
        """Recursively propagate label removal to all descendants."""
        children = self.api.get_children(source)
        
        for child in children:
            if child in visited:
                continue
            visited.add(child)
            
            child_metadata = self.label_ops.get_label_metadata(child, label)
            
            if child_metadata is None:
                continue
            
            sources = self.source_manager.remove_source(child_metadata.sources, source)
            
            if not self.source_manager.has_sources(sources):
                # No more sources, remove completely
                self.label_ops.remove_label_if_exists(child, label)
                self._propagate_removal(child, label, visited)
            else:
                # Still has other sources, keep it
                updated_metadata = LabelMetadata(
                    added_at=child_metadata.added_at,
                    direct=child_metadata.direct,
                    sources=sources
                )
                self.label_ops.add_label_with_metadata(child, label, updated_metadata)
    
    def add_edge(self, parent: str, child: str) -> None:
        """Add an edge and propagate all parent labels to child."""
        self.api.add_edge(parent, child)
        
        # Propagate all parent labels
        parent_labels = self.api.get_labels(parent)
        for label in parent_labels.keys():
            visited = set()
            self.propagation_strategy.propagate(self.api, parent, label, visited)
    
    def remove_edge(self, parent: str, child: str) -> None:
        """Remove an edge and clean up propagated labels."""
        self.api.remove_edge(parent, child)
        
        # Remove parent as source from all its labels on child
        parent_labels = self.api.get_labels(parent)
        for label in parent_labels.keys():
            child_metadata = self.label_ops.get_label_metadata(child, label)
            
            if child_metadata is not None:
                sources = self.source_manager.remove_source(child_metadata.sources, parent)
                
                if not self.source_manager.has_sources(sources):
                    self.label_ops.remove_label_if_exists(child, label)
                    visited = set()
                    self._propagate_removal(child, label, visited)
                else:
                    updated_metadata = LabelMetadata(
                        added_at=child_metadata.added_at,
                        direct=child_metadata.direct,
                        sources=sources
                    )
                    self.label_ops.add_label_with_metadata(child, label, updated_metadata)


# Unnecessary abstraction layer 5: Time utility
class TimeUtility:
    """Utility for time-based operations."""
    
    @staticmethod
    def get_cutoff_time(hours: int) -> datetime:
        """Get cutoff time for the given hours ago."""
        now = datetime.now()
        delta = timedelta(hours=hours)
        cutoff = now - delta
        return cutoff
    
    @staticmethod
    def is_recent(timestamp: Optional[datetime], cutoff: datetime) -> bool:
        """Check if timestamp is after cutoff."""
        if timestamp is None:
            return False
        else:
            if timestamp >= cutoff:
                return True
            else:
                return False


# Unnecessary abstraction layer 6: Label filter
class LabelFilter:
    """Filters labels based on criteria."""
    
    def __init__(self, time_utility: TimeUtility):
        self.time_utility = time_utility
    
    def filter_recent_direct_labels(self, labels: dict[str, dict], hours: int) -> dict[str, dict]:
        """Filter labels to only recent direct ones."""
        cutoff = self.time_utility.get_cutoff_time(hours)
        
        result = {}
        for label in labels:
            props = labels[label]
            if 'direct' in props:
                if props['direct'] == True:
                    if 'added_at' in props:
                        if self.time_utility.is_recent(props['added_at'], cutoff):
                            result[label] = props
        
        return result


class LabelService:
    """External-facing service for interacting with labels."""
    
    def __init__(self, graph_service: GraphService):
        self.graph_service = graph_service
        self.time_utility = TimeUtility()
        self.label_filter = LabelFilter(self.time_utility)
    
    def get_recently_added_labels(self, node: str) -> dict[str, dict]:
        """Returns labels added directly to this node in the last 24 hours."""
        all_labels = self.graph_service.get_labels(node)
        return self.label_filter.filter_recent_direct_labels(all_labels, 24)
    
    def get_node_labels(self, node: str) -> dict[str, dict]:
        """Returns all labels on this node (including inherited)."""
        return self.graph_service.get_labels(node)
