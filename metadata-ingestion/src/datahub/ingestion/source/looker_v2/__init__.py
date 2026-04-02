"""
Looker V2 Combined Source.

This module provides a unified source for ingesting metadata from Looker,
combining functionality from both the legacy `looker` and `lookml` sources.

Key features:
- Hybrid extraction: API-based for dashboards/explores, file-based for views
- Support for unreachable views (included but not referenced by explores)
- Multi-project dependency resolution
- View refinement support with lineage attribution
- Fine-grained statistics and API telemetry
"""
