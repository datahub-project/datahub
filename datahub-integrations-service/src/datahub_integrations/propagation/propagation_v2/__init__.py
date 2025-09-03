"""
Second version of the generic propagation action, created to avoid backwards compatibility issues
Changes include:
- A more performant and horizontally-scalable bootstrap
- Support for non-lineage relationship lookups
- Support for propagation on the creation / deletion of a relationship, by listening to the RelationshipChangeEvent
- Schema field propagation solely through aspects on schema field entities, rather than their parent dataset
"""
