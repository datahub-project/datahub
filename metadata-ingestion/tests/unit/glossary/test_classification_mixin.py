from typing import Any, Dict, List

import pytest

from datahub.ingestion.glossary.classification_mixin import (
    ClassificationHandler,
    ClassificationReportMixin,
    ClassificationSourceConfigMixin,
)
from datahub.ingestion.glossary.classification_types import ColumnInfo
from datahub.ingestion.glossary.classifier import (
    ClassificationConfig,
    Classifier,
    DynamicTypedClassifierConfig,
)
from datahub.ingestion.glossary.classifier_registry import classifier_registry
from datahub.metadata.schema_classes import (
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)


class RecordingClassifier(Classifier):
    def __init__(self) -> None:
        self.columns: List[ColumnInfo] = []

    def classify(self, columns: List[ColumnInfo]) -> List[ColumnInfo]:
        self.columns.extend(columns)
        return columns

    @classmethod
    def create(cls, config_dict: Dict[str, Any]) -> "RecordingClassifier":
        return cls()


def test_classifier_receives_column_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(
        classifier_registry.mapping, "test-recording", RecordingClassifier
    )
    config = ClassificationSourceConfigMixin(
        classification=ClassificationConfig(
            enabled=True,
            max_workers=1,
            classifiers=[DynamicTypedClassifierConfig(type="test-recording")],
        )
    )
    handler = ClassificationHandler(config, ClassificationReportMixin())
    schema_metadata = SchemaMetadataClass(
        schemaName="my_table",
        platform="urn:li:dataPlatform:postgres",
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath="col_a",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="VARCHAR(255)",
                description="Contact email",
            )
        ],
    )

    handler.classify_schema_fields(
        "my_db.my_schema.my_table", schema_metadata, {"col_a": ["a@example.com"]}
    )

    [classifier] = handler.classifiers
    assert isinstance(classifier, RecordingClassifier)
    [column] = classifier.columns
    assert column.metadata.name == "col_a"
    assert column.metadata.description == "Contact email"
    assert column.metadata.datatype == "VARCHAR(255)"
    assert column.metadata.dataset_name == "my_db.my_schema.my_table"
    assert column.values == ["a@example.com"]
