from datahub.ingestion.source.powerbi.rest_api_wrapper.composite_model import (
    analysis_services_targets,
    parse_remote_model_reference,
    remote_model_key,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    RemoteModelReference,
)

DATASET_ID = "7A1B2C3D-4E5F-4A6B-8C7D-9E0F1A2B3C4D"

# Power BI Desktop's classic shape: workspace in the URL, model name as the
# database, the cube picked by its fixed "Model" id.
BY_NAME = """let
    Source = AnalysisServices.Database("powerbi://api.powerbi.com/v1.0/myorg/Sales Analytics", "Sales Model"),
    Cubes = Table.Combine(Source[Data]),
    Cube = Cubes{[Id="Model"]}[Data]
in
    Cube"""

# Newer shape: the dataset's XMLA catalog name in the options record.
WITH_CATALOG = """let
    Source = AnalysisServices.Database("powerbi://api.powerbi.com/v1.0/myorg/Sales%20Analytics", "Sales Model", [Catalog="sobe_wowvirtualserver-7a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d", Culture="en-US"]),
    Cubes = Table.Combine(Source[Data]),
    Cube = Cubes{[Id="Model"]}[Data]
in
    Cube"""

# The dataset id as the database argument: the model name is only in the
# expression's own title.
BY_ID = """let
    Source = AnalysisServices.Database("powerbi://api.powerbi.com/v1.0/myorg/Sales Analytics", "7A1B2C3D-4E5F-4A6B-8C7D-9E0F1A2B3C4D")
in
    Source"""


def test_parse_by_workspace_and_model_name() -> None:
    reference = parse_remote_model_reference("DirectQuery to AS - Sales Model", BY_NAME)
    assert reference == RemoteModelReference(
        workspace_name="Sales Analytics", model_name="Sales Model", dataset_id=None
    )


def test_parse_with_catalog_dataset_id_and_encoded_workspace_name() -> None:
    reference = parse_remote_model_reference(
        "DirectQuery to AS - Sales Model", WITH_CATALOG
    )
    assert reference == RemoteModelReference(
        workspace_name="Sales Analytics",
        model_name="Sales Model",
        dataset_id="7a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d",
    )


def test_parse_https_endpoint_host_counts_as_power_bi() -> None:
    reference = parse_remote_model_reference(
        "Remote",
        'AnalysisServices.Database("https://analysis.windows.net/powerbi/api/v1.0/myorg/Sales Analytics", "Sales Model")',
    )
    assert reference == RemoteModelReference("Sales Analytics", "Sales Model")


def test_parse_dataset_id_as_database_takes_name_from_expression_title() -> None:
    reference = parse_remote_model_reference("DirectQuery to AS - Sales Model", BY_ID)
    assert reference == RemoteModelReference(
        workspace_name="Sales Analytics",
        model_name="Sales Model",
        dataset_id=DATASET_ID.casefold(),
    )
    # Without the Desktop title the id stays the name; only the id resolves.
    untitled = parse_remote_model_reference("Remote model", BY_ID)
    assert untitled == RemoteModelReference(
        workspace_name="Sales Analytics",
        model_name=DATASET_ID,
        dataset_id=DATASET_ID.casefold(),
    )


def test_other_expressions_are_not_remote_models() -> None:
    assert (
        parse_remote_model_reference("Param", '"value" meta [IsParameterQuery=true]')
        is None
    )
    assert (
        parse_remote_model_reference(
            "DatabaseQuery", 'Sql.Database("endpoint.fabric.microsoft.com", "id")'
        )
        is None
    )
    # Azure or on-premises Analysis Services is not a Power BI dataset.
    assert (
        parse_remote_model_reference(
            "Cube", 'AnalysisServices.Database("ssas.internal", "Sales")'
        )
        is None
    )
    assert (
        parse_remote_model_reference(
            "DirectQuery to AS - Sales",
            'AnalysisServices.Database("asazure://westus.asazure.windows.net/finance", "Sales")',
        )
        is None
    )


def test_analysis_services_targets_distinguish_power_bi_from_other_servers() -> None:
    targets = analysis_services_targets(
        {
            "DirectQuery to AS - Sales Model": BY_NAME,
            "DirectQuery to AS - Finance": 'let Source = AnalysisServices.Database("asazure://westus.asazure.windows.net/finance", "Finance") in Source',
            "Param": '"value" meta [IsParameterQuery=true]',
        }
    )
    assert set(targets) == {
        "DirectQuery to AS - Sales Model",
        "DirectQuery to AS - Finance",
    }
    assert targets["DirectQuery to AS - Sales Model"] is not None
    assert targets["DirectQuery to AS - Finance"] is None


def test_remote_model_key_folds_case_and_whitespace() -> None:
    assert remote_model_key(" Sales Analytics", "SALES model ") == (
        "sales analytics/sales model"
    )
