from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.vertica import VerticaConfig, VerticaSource
from datahub.metadata.schema_classes import MetadataChangeEventClass
from tests.unit.test_vertica_workunits import _inspector


def test_projections_and_models_emit_no_mce() -> None:
    # The only Vertica golden test is skip-marked, so this is what fails if
    # projections or models go back to MCEs. It calls the loops directly because
    # tables and views come from SQLAlchemySource, which this test does not cover.
    inspector = _inspector()
    source = VerticaSource(
        VerticaConfig.model_validate({"host_port": "host:5433", "database": "vmart"}),
        PipelineContext(run_id="vertica-mcp-only-test"),
    )
    workunits = [
        *source.loop_projections(inspector, "main", source.config),
        *source.loop_models(inspector, "main", source.config),
    ]

    assert not source.report.failures
    assert not source.report.warnings
    urns = {wu.get_urn() for wu in workunits}
    assert {
        "urn:li:dataset:(urn:li:dataPlatform:vertica,vmart.main.orders_super,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:vertica,vmart.main.orders_b0,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:vertica,vmart.Entities.churn,PROD)",
    } <= urns
    assert not [
        wu for wu in workunits if isinstance(wu.metadata, MetadataChangeEventClass)
    ]
