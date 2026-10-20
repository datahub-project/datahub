from typing import Any, Dict, List
from unittest import mock

import requests

from datahub.ingestion.source.tableau.tableau import (
    TableauConfig,
    TableauSourceReport,
)
from datahub.ingestion.source.tableau.tableau_pulse import TableauPulseExtractor
from datahub.metadata.schema_classes import SchemaMetadataClass, SubTypesClass


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


def _snapshots(workunits: List[Any]) -> List[Any]:
    return [
        wu.metadata.proposedSnapshot
        for wu in workunits
        if hasattr(wu.metadata, "proposedSnapshot")
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


def test_definition_is_a_dataset_and_metrics_are_columns() -> None:
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
                                    "granularity": "GRANULARITY_BY_MONTH"
                                },
                            },
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
                            },
                            "specification": {"datasource": {"id": "ds-1"}},
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

    snapshots = _snapshots(workunits)
    assert [snapshot.urn for snapshot in snapshots] == [
        "urn:li:dataset:(urn:li:dataPlatform:tableau,def-1,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:tableau,def-2,PROD)",
    ]
    sales = snapshots[0]
    properties = next(
        aspect for aspect in sales.aspects if getattr(aspect, "name", None) == "Sales"
    )
    assert properties.description == "Sales pulse"
    assert properties.customProperties == {
        "definitionId": "def-1",
        "datasourceId": "ds-1",
    }
    schema = next(
        aspect for aspect in sales.aspects if isinstance(aspect, SchemaMetadataClass)
    )
    assert [field.fieldPath for field in schema.fields] == [
        "default",
        "Region=West | by month",
    ]
    assert schema.fields[1].nativeDataType == "metric"
    subtypes = [
        wu.metadata.aspect
        for wu in workunits
        if isinstance(getattr(wu.metadata, "aspect", None), SubTypesClass)
    ]
    assert subtypes[0].typeNames == ["Metric Definition"]
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
    assert len(_snapshots(workunits)) == 2
    assert len(extractor.report.warnings) == 1


def test_metrics_failure_still_emits_the_definition() -> None:
    def get(url: str, **kwargs: Any) -> _Response:
        if url.endswith("/metrics"):
            return _Response({}, status_code=500)
        return _Response(
            {"definitions": [{"metadata": {"id": "def-1", "name": "Sales"}}]}
        )

    extractor = _extractor(get)
    snapshots = _snapshots(list(extractor.emit()))
    assert len(snapshots) == 1
    assert not any(
        isinstance(aspect, SchemaMetadataClass) for aspect in snapshots[0].aspects
    )
    assert extractor.report.num_pulse_definitions == 1
    assert len(extractor.report.warnings) == 1
