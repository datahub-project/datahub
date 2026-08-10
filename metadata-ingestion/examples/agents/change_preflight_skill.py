import logging
from datahub.ingestion.graph.client import get_default_graph
from datahub.sdk.document import Document

log = logging.getLogger(__name__)

def run_preflight_skill(
    graph,
    dataset_urn: str,
    proposed_schema_digest: str,
    run_id: str,
) -> str:
    """
    Demonstrates an AI Agent skill that evaluates a proposed schema change 
    and writes a "Safety Case" Context Document back to DataHub.
    """
    log.info(f"Running preflight check on {dataset_urn}")

    # 1. Simulate agentic reasoning (fetching lineage, historical incidents, etc.)
    # In a real agent, you would query graph.get_lineage, graph.get_incidents, etc.
    analysis_text = f"""# Preflight Safety Case: `{dataset_urn}`

## Semantic Contract
The proposed change (`digest: {proposed_schema_digest}`) removes a column `user_id`.

## Impact Analysis
- **Downstream Assets**: 12 dashboards, 3 ML models.
- **Risk Level**: HIGH.

## Decision
**BLOCKED**: Removing `user_id` breaks the downstream fraud detection ML model.
"""

    # 2. Write the safety case as a native Context Document using the SDK
    document_urn_id = f"safety-case-{run_id}"
    doc = Document.create_document(
        id=document_urn_id,
        title=f"Preflight Check: {run_id}",
        text=analysis_text,
        subtype="Safety Case",
        show_in_global_context=False,
        related_assets=[dataset_urn]
    )

    for mcp in doc.generate_mcp():
        graph.emit(mcp)

    log.info(f"Emitted Safety Case Document: urn:li:document:{document_urn_id}")
    return document_urn_id

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    graph = get_default_graph()
    
    # Run the demo preflight
    run_preflight_skill(
        graph,
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,ecommerce.orders,PROD)",
        proposed_schema_digest="sha256:abc123def456",
        run_id="ci-run-98765"
    )
