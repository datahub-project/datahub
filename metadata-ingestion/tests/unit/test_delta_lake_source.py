from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.delta_lake import DeltaLakeSource, DeltaLakeSourceConfig, QueryTableMetadataResponse_extended
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    MapTypeClass,
    NumberTypeClass,
    StringTypeClass,
    UnionTypeClass,
)

from delta_sharing.protocol import Table, Format
from delta_sharing.rest_client import Metadata, Protocol

import pytest

@pytest.fixture
def testdata1():
    return QueryTableMetadataResponse_extended(protocol=Protocol(min_reader_version=1), metadata=Metadata(id='test1', name=None, description=None, format=Format(provider='parquet', options={}), schema_string='{"type":"struct","fields":[{"name":"date","type":"string","nullable":true,"metadata":{}},{"name":"county","type":"string","nullable":true,"metadata":{}},{"name":"state","type":"string","nullable":true,"metadata":{}},{"name":"fips","type":"integer","nullable":true,"metadata":{}},{"name":"cases","type":"integer","nullable":true,"metadata":{}},{"name":"deaths","type":"integer","nullable":true,"metadata":{}}]}', partition_columns=[]), table=Table(name='testdata1', share='delta_sharing', schema='default'))

@pytest.fixture
def testdata2():
    return QueryTableMetadataResponse_extended(protocol=Protocol(min_reader_version=1), metadata=Metadata(id='test2', name=None, description=None, format=Format(provider='parquet', options={}), schema_string='{"type":"struct","fields":[{"name":"a","type":"integer","nullable":false,"metadata":{"comment":"this is a comment"}},{"name":"b","type":{"type":"struct","fields":[{"name":"d","type":"integer","nullable":false,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"c","type":{"type":"array","elementType":"integer","containsNull":false},"nullable":true,"metadata":{}},{"name":"e","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"d","type":"integer","nullable":false,"metadata":{}}]},"containsNull":true},"nullable":true,"metadata":{}},{"name":"f","type":{"type":"map","keyType":"string","valueType":"string","valueContainsNull":true},"nullable":true,"metadata":{}}]}', partition_columns=[]), table=Table(name='testdata2', share='delta_sharing', schema='default'))

def test_platform_correctly_set_delta_lake():
    source = DeltaLakeSource(
        ctx=PipelineContext(run_id="delta-lake-source-test1"),
        config=DeltaLakeSourceConfig(url="url", token="token"),
    )
    assert source.platform == "deltalake"

def test_get_schema_fields():
    source = DeltaLakeSource(
        ctx=PipelineContext(run_id="delta-lake-source-test2"),
        config=DeltaLakeSourceConfig(url="url",
        token="x"))
    schema_fields=source._get_schema_fields(testdata1().metadata)
    
    assert schema_fields == [SchemaField({'fieldPath': 'date', 'jsonPath': None, 'nullable': True, 'description': None, 'type': StringTypeClass, 'nativeDataType': 'string', 'recursive': False, 'globalTags': None, 'glossaryTerms': None, 'isPartOfKey': False, 'jsonProps': None}),
    SchemaField({'fieldPath': 'county', 'jsonPath': None, 'nullable': True, 'description': None, 'type': StringTypeClass, 'nativeDataType': 'string', 'recursive': False, 'globalTags': None, 'glossaryTerms': None, 'isPartOfKey': False, 'jsonProps': None}),
    SchemaField({'fieldPath': 'state', 'jsonPath': None, 'nullable': True, 'description': None, 'type': StringTypeClass, 'nativeDataType': 'string', 'recursive': False, 'globalTags': None, 'glossaryTerms': None, 'isPartOfKey': False, 'jsonProps': None}),
    SchemaField({'fieldPath': 'fips', 'jsonPath': None, 'nullable': True, 'description': None, 'type': NumberTypeClass, 'nativeDataType': 'integer', 'recursive': False, 'globalTags': None, 'glossaryTerms': None, 'isPartOfKey': False, 'jsonProps': None}),
    SchemaField({'fieldPath': 'cases', 'jsonPath': None, 'nullable': True, 'description': None, 'type': NumberTypeClass, 'nativeDataType': 'integer', 'recursive': False, 'globalTags': None, 'glossaryTerms': None, 'isPartOfKey': False, 'jsonProps': None}),
    SchemaField({'fieldPath': 'deaths', 'jsonPath': None, 'nullable': True, 'description': None, 'type': NumberTypeClass, 'nativeDataType': 'integer', 'recursive': False, 'globalTags': None, 'glossaryTerms': None, 'isPartOfKey': False, 'jsonProps': None}),]

def test_get_schema_fields_nested():
    source = DeltaLakeSource(
        ctx=PipelineContext(run_id="delta-lake-source-test3"),
        config=DeltaLakeSourceConfig(url="url",
        token="x"))
    schema_fields=source._get_schema_fields(testdata2().metadata)

    assert schema_fields == [SchemaField({'fieldPath': 'a', 'jsonPath': None, 'nullable': False, 'description': 'this is a comment', 'type': NumberTypeClass, 'nativeDataType': 'integer', 'recursive': False, 'globalTags': None, 'glossaryTerms': None, 'isPartOfKey': False, 'jsonProps': None}),
    SchemaField({'fieldPath': 'b', 'jsonPath': None, 'nullable': True, 'description': None, 'type': UnionTypeClass, 'nativeDataType': 'struct', 'recursive': False, 'globalTags': None, 'glossaryTerms': None, 'isPartOfKey': False, 'jsonProps': '{"type": "struct", "fields": [{"name": "d", "type": "integer", "nullable": false, "metadata": {}}]}'}),
    SchemaField({'fieldPath': 'c', 'jsonPath': None, 'nullable': True, 'description': None, 'type': ArrayTypeClass, 'nativeDataType': 'array', 'recursive': False, 'globalTags': None, 'glossaryTerms': None, 'isPartOfKey': False, 'jsonProps': '{"type": "array", "elementType": "integer", "containsNull": false}'}),
    SchemaField({'fieldPath': 'e', 'jsonPath': None, 'nullable': True, 'description': None, 'type': ArrayTypeClass, 'nativeDataType': 'array', 'recursive': False, 'globalTags': None, 'glossaryTerms': None, 'isPartOfKey': False, 'jsonProps': '{"type": "array", "elementType": {"type": "struct", "fields": [{"name": "d", "type": "integer", "nullable": false, "metadata": {}}]}, "containsNull": true}'}),
    SchemaField({'fieldPath': 'f', 'jsonPath': None, 'nullable': True, 'description': None, 'type': MapTypeClass, 'nativeDataType': 'map', 'recursive': False, 'globalTags': None, 'glossaryTerms': None, 'isPartOfKey': False, 'jsonProps': '{"type": "map", "keyType": "string", "valueType": "string", "valueContainsNull": true}'}),]
