from datahub.cli import query_cli


def test_get_aspect_from_entity():
    ownership_aspect = {
        "owners": [
            {"owner": "urn:li:corpuser:jdoe", "type": "DATAOWNER"},
            {
                "owner": "urn:li:corpuser:datahub",
                "type": "DATAOWNER",
            },
        ],
        "lastModified": {
            "actor": "urn:li:corpuser:jdoe",
            "time": 1581407189000,
        },
    }
    entity = {
        "value": {
            "com.linkedin.metadata.snapshot.DatasetSnapshot": {
                "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
                "aspects": [
                    {"com.linkedin.common.Ownership": ownership_aspect},
                ],
            }
        }
    }
    result = query_cli._get_aspect_from_entity(entity, "com.linkedin.common.Ownership")
    assert result == ownership_aspect


def test_get_empty_aspect_from_entity():
    entity = {
        "value": {
            "com.linkedin.metadata.snapshot.DatasetSnapshot": {
                "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
                "aspects": [],
            }
        }
    }
    result = query_cli._get_aspect_from_entity(entity, "com.linkedin.common.Ownership")
    assert result == {}
