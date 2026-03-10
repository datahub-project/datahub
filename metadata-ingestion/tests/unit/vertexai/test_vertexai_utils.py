from datahub.ingestion.source.vertexai.vertexai_builder import (
    VertexAIExternalURLBuilder,
    VertexAINameFormatter,
)


def test_vertexai_multi_project_context_naming() -> None:
    name_formatter = VertexAINameFormatter(get_project_id_fn=lambda: "p2")
    url_builder = VertexAIExternalURLBuilder(
        base_url="https://console.cloud.google.com/vertex-ai",
        get_project_id_fn=lambda: "p2",
        get_region_fn=lambda: "r2",
    )

    assert name_formatter.format_model_group_name("m") == "p2.model_group.m"
    assert name_formatter.format_dataset_name("d") == "p2.dataset.d"

    assert (
        url_builder.make_model_url("xyz")
        == "https://console.cloud.google.com/vertex-ai/models/locations/r2/models/xyz?project=p2"
    )
