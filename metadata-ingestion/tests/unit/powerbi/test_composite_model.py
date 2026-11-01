from datahub.ingestion.source.powerbi.rest_api_wrapper.composite_model import (
    parse_remote_model_reference,
    remote_model_key,
    remote_models_from_expressions,
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


def test_parse_dataset_id_as_database_takes_name_from_expression_title() -> None:
    reference = parse_remote_model_reference("DirectQuery to AS - Sales Model", BY_ID)
    assert reference == RemoteModelReference(
        workspace_name="Sales Analytics",
        model_name="Sales Model",
        dataset_id=DATASET_ID,
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
    # An on-premises Analysis Services server is not a Power BI dataset.
    assert (
        parse_remote_model_reference(
            "Cube", 'AnalysisServices.Database("ssas.internal", "Sales")'
        )
        is None
    )


def test_remote_models_from_expressions_keeps_only_remote_models() -> None:
    remote_models = remote_models_from_expressions(
        {
            "DirectQuery to AS - Sales Model": BY_NAME,
            "Param": '"value" meta [IsParameterQuery=true]',
        }
    )
    assert set(remote_models) == {"DirectQuery to AS - Sales Model"}


def test_remote_model_key_folds_case_and_whitespace() -> None:
    assert remote_model_key(" Sales Analytics", "SALES model ") == (
        "sales analytics/sales model"
    )
