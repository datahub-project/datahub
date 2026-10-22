from typing import Any, Dict, List
from unittest import mock

import requests

from datahub.ingestion.source.tableau.tableau import (
    TableauConfig,
    TableauSourceReport,
)
from datahub.ingestion.source.tableau.tableau_pulse import TableauPulseExtractor
from datahub.metadata.schema_classes import (
    MetricInfoClass,
    MetricRelationshipsClass,
    SemanticModelInfoClass,
    SubTypesClass,
)


class _Response:
    def __init__(self, payload: Dict[str, Any], status_code: int = 200) -> None:
        self.status_code = status_code
        self._payload = payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)

    def json(self) -> Dict[str, Any]:
        return self._payload


def _config() -> TableauConfig:
    return TableauConfig.model_validate(
        {
            "username": "user",
            "password": "pass",
            "connect_uri": "https://tableau.example",
            "site": "site",
            "extract_pulse_metrics": True,
            "page_size": 1,
        }
    )


def _extractor(get: Any) -> TableauPulseExtractor:
    server = mock.Mock()
    server.auth_token = "token"
    server._session.get.side_effect = get
    return TableauPulseExtractor(
        config=_config(),
        server=server,
        report=TableauSourceReport(),
        platform="tableau",
        browse_prefix="/prod/tableau",
    )


def _aspects(workunits: List[Any], aspect_type: type) -> List[Any]:
    return [
        wu.metadata.aspect
        for wu in workunits
        if isinstance(getattr(wu.metadata, "aspect", None), aspect_type)
    ]


def test_extract_pulse_metrics_defaults_off() -> None:
    config = TableauConfig.model_validate(
        {
            "username": "user",
            "password": "pass",
            "connect_uri": "https://tableau.example",
            "site": "site",
        }
    )
    assert config.extract_pulse_metrics is False


def test_definition_is_a_semantic_model_and_metrics_are_metrics() -> None:
    def get(url: str, **kwargs: Any) -> _Response:
        if url.endswith("/metrics"):
            return _Response(
                {
                    "metrics": [
                        {
                            "id": "metric-default",
                            "is_default": True,
                            "specification": {},
                        },
                        {
                            "id": "metric-west",
                            "is_default": False,
                            "specification": {
                                "filters": [
                                    {
                                        "field": "Region",
                                        "categorical_values": [
                                            {"string_value": "West"}
                                        ],
                                    }
                                ],
                                "measurement_period": {
                                    "granularity": "GRANULARITY_BY_MONTH",
                                    "range": "RANGE_UNSPECIFIED",
                                },
                                "comparison": {
                                    "comparison": "TIME_COMPARISON_PREVIOUS_PERIOD"
                                },
                            },
                            "datasource_luid": "luid-1",
                        },
                        {"is_default": False},
                    ]
                }
            )
        token = kwargs["params"].get("page_token")
        if token is None:
            return _Response(
                {
                    "definitions": [
                        {
                            "metadata": {
                                "id": "def-1",
                                "name": "Sales",
                                "description": "Sales pulse",
                                "schema_version": "1.2",
                                "created_at": "2024-01-01T00:00:00Z",
                            },
                            "specification": {
                                "datasource": {"id": "ds-1"},
                                "basic_specification": {
                                    "measure": {
                                        "field": "Sales Amount",
                                        "aggregation": "AGGREGATION_SUM",
                                    },
                                    "time_dimension": {"field": "Order Date"},
                                },
                                "temporality": "TEMPORALITY_OVER_TIME",
                                "is_running_total": False,
                            },
                        }
                    ],
                    "next_page_token": "page-2",
                }
            )
        return _Response(
            {
                "definitions": [
                    {"metadata": {"id": "def-2", "name": "Profit", "description": None}}
                ]
            }
        )

    extractor = _extractor(get)
    workunits = list(extractor.emit())

    models = _aspects(workunits, SemanticModelInfoClass)
    assert [model.name for model in models] == ["Sales", "Profit"]
    assert models[0].description == "Sales pulse"
    assert models[0].nativeDefinition == "\n".join(
        [
            "measure: SUM(Sales Amount)",
            "time_dimension: Order Date",
            "temporality: OVER_TIME",
            "is_running_total: false",
            "datasource_id: ds-1",
        ]
    )
    model_urns = [
        wu.metadata.entityUrn
        for wu in workunits
        if isinstance(getattr(wu.metadata, "aspect", None), SemanticModelInfoClass)
    ]
    assert model_urns == [
        "urn:li:semanticModel:(urn:li:dataPlatform:tableau,site,def-1)",
        "urn:li:semanticModel:(urn:li:dataPlatform:tableau,site,def-2)",
    ]
    metrics = [
        (wu.metadata.entityUrn, wu.metadata.aspect)
        for wu in workunits
        if isinstance(getattr(wu.metadata, "aspect", None), MetricInfoClass)
    ]
    sales_metrics = [item for item in metrics if ",site.def-1," in item[0]]
    assert [metric.name for _, metric in sales_metrics] == [
        "Sales",
        "Sales · Region=West | by month",
    ]
    default_metric = sales_metrics[0][1]
    assert default_metric.semanticModel == model_urns[0]
    assert default_metric.expression.dialects[0].dialect == "TABLEAU"
    assert default_metric.expression.dialects[0].expression == "SUM([Sales Amount])"
    assert "Default metric for this definition." in default_metric.description
    assert "Measure: SUM(Sales Amount)" in default_metric.description
    filtered = sales_metrics[1][1]
    assert "Filters: Region=West" in filtered.description
    assert "Granularity: BY_MONTH" in filtered.description
    assert "Comparison: PREVIOUS_PERIOD" in filtered.description
    assert "Datasource: luid-1" in filtered.description
    relationships = _aspects(workunits, MetricRelationshipsClass)
    assert relationships
    assert all(aspect.parentMetric is None for aspect in relationships)
    assert _aspects(workunits, SubTypesClass)[0].typeNames == [
        "Pulse Metric Definition"
    ]
    assert extractor.report.num_pulse_definitions == 2
    assert extractor.report.num_pulse_metrics == 4
    assert len(extractor.report.warnings) == 1


def test_pulse_listing_failure_warns_and_emits_nothing() -> None:
    extractor = _extractor(lambda url, **kwargs: _Response({}, status_code=404))
    assert list(extractor.emit()) == []
    assert len(extractor.report.warnings) == 1
    assert extractor.report.num_pulse_definitions == 0


def test_repeated_page_token_stops_listing() -> None:
    calls = {"n": 0}

    def get(url: str, **kwargs: Any) -> _Response:
        calls["n"] += 1
        if url.endswith("/metrics"):
            return _Response({"metrics": []})
        return _Response(
            {
                "definitions": [{"metadata": {"id": "def-1", "name": "Sales"}}],
                "next_page_token": "again",
            }
        )

    extractor = _extractor(get)
    workunits = list(extractor.emit())
    # Two definition pages, then metrics for each emitted definition.
    assert calls["n"] == 4
    assert len(_aspects(workunits, SemanticModelInfoClass)) == 2
    assert len(extractor.report.warnings) == 1


def test_metrics_failure_still_emits_the_definition() -> None:
    def get(url: str, **kwargs: Any) -> _Response:
        if url.endswith("/metrics"):
            return _Response({}, status_code=500)
        return _Response(
            {"definitions": [{"metadata": {"id": "def-1", "name": "Sales"}}]}
        )

    extractor = _extractor(get)
    workunits = list(extractor.emit())
    assert len(_aspects(workunits, SemanticModelInfoClass)) == 1
    assert _aspects(workunits, MetricInfoClass) == []
    assert extractor.report.num_pulse_definitions == 1
    assert len(extractor.report.warnings) == 1
