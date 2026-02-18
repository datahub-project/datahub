from datahub.ingestion.source.vertexai.vertexai_builder import (
    VertexAIExternalURLBuilder,
    VertexAINameFormatter,
)


def test_vertexai_multi_project_context_naming() -> None:
    name_formatter = VertexAINameFormatter(project_id="p2")
    url_builder = VertexAIExternalURLBuilder(
        base_url="https://console.cloud.google.com/vertex-ai",
        project_id="p2",
        region="r2",
    )

    assert name_formatter.format_model_group_name("m") == "p2.model_group.m"
    assert name_formatter.format_dataset_name("d") == "p2.dataset.d"
    assert name_formatter.format_pipeline_task_id("t") == "p2.pipeline_task.t"

    assert (
        url_builder.make_model_url("xyz")
        == "https://console.cloud.google.com/vertex-ai/models/locations/r2/models/xyz?project=p2"
    )
