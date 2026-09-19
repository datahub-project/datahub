# This example does not decide whether a proposed change is safe.
#
# The calling agent or CI system is responsible for producing the
# disposition and evidence summary. This example only demonstrates
# persisting that already-derived decision into DataHub and verifying
# the write through readback.

from datahub.metadata.urns import DocumentUrn
from datahub.sdk import DataHubClient, Document


def write_preflight_safety_case(
    client: DataHubClient,
    *,
    run_id: str,
    asset_urn: str,
    change_digest: str,
    disposition: str,
    summary: str,
) -> DocumentUrn:
    """Persist and verify an externally-derived preflight decision."""

    document_id = f"preflight-safety-case-{run_id}"

    text = f"""# Preflight Safety Case

## Run

`{run_id}`

## Asset

`{asset_urn}`

## Proposed Change Digest

`{change_digest}`

## Disposition

**{disposition}**

## Evidence Summary

{summary}
"""

    document = Document.create_document(
        id=document_id,
        title=f"Preflight Safety Case: {run_id}",
        text=text,
        subtype="Safety Case",
        show_in_global_context=False,
        related_assets=[asset_urn],
        custom_properties={
            "run_id": run_id,
            "change_digest": change_digest,
            "disposition": disposition,
        },
    )

    client.entities.upsert(document)

    persisted = client.entities.get(document.urn)

    if persisted is None:
        raise RuntimeError(
            f"Safety Case was not readable after write: {document.urn}"
        )

    related_assets = {str(asset) for asset in persisted.related_assets}

    if asset_urn not in related_assets:
        raise RuntimeError(
            "Safety Case readback did not preserve the related asset"
        )

    return document.urn


if __name__ == "__main__":
    client = DataHubClient.from_env()

    urn = write_preflight_safety_case(
        client,
        run_id="ci-run-98765",
        asset_urn=(
            "urn:li:dataset:"
            "(urn:li:dataPlatform:snowflake,ecommerce.orders,PROD)"
        ),
        change_digest="sha256:abc123def456",
        disposition="BLOCKED",
        summary=(
            "Example evidence supplied by the calling preflight workflow. "
            "Replace this with evidence derived from your own agent or CI system."
        ),
    )

    print(f"Verified Safety Case writeback: {urn}")
