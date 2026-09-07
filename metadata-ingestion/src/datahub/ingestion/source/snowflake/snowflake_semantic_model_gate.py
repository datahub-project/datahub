# The server-capability gate is shared with other connectors that emit
# semanticModel/metric entities; it reads no Snowflake config. Re-exported here
# so existing Snowflake imports keep working.
from datahub.ingestion.source.common.semantic_model_gate import (
    ResolvedEmitDecision,
    resolve_emit_semantic_model_entities,
)

__all__ = ["ResolvedEmitDecision", "resolve_emit_semantic_model_entities"]
