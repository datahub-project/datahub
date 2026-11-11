import json

from datahub.metadata.schema_classes import ChangeTypeClass, EmbeddingModelDataClass
from datahub.specific.semantic_content import SemanticContentPatchBuilder


def test_patch_names_one_profile():
    profile = EmbeddingModelDataClass(
        modelVersion="model-b",
        generatedAt=1,
        totalChunks=0,
        chunks=[],
    )
    mcps = (
        SemanticContentPatchBuilder("urn:li:document:doc")
        .set_profile("profileB", profile)
        .build()
    )

    assert len(mcps) == 1
    assert mcps[0].changeType == ChangeTypeClass.PATCH
    assert mcps[0].aspect is not None
    payload = json.loads(mcps[0].aspect.value)
    assert payload[0]["path"] == "/embeddings/profileB"
    assert "profileA" not in json.dumps(payload)
