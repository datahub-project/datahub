from types import SimpleNamespace
from typing import Any, Dict, List

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.salesforce_probe import list_salesforce_children


class _SalesforceClient:
    def __init__(self, objects: List[Dict[str, Any]]) -> None:
        self._objects = objects

    def list_objects(self) -> List[Dict[str, Any]]:
        return self._objects


def _config():
    objects = [
        {"QualifiedApiName": "Account"},
        {"QualifiedApiName": "MyCustom__c"},
        {"QualifiedApiName": "tmp_scratch__c"},
    ]
    return SimpleNamespace(
        get_client=lambda: _SalesforceClient(objects),
        object_pattern=AllowDenyPattern(allow=[".*"], deny=["^tmp_.*"]),
    )


def test_salesforce_lists_standard_object_with_pattern_verdict():
    result = list_salesforce_children(_config(), [], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["Account"].kind == DatasetSubTypes.SALESFORCE_STANDARD_OBJECT
    assert by_name["Account"].pattern_field == "object_pattern"
    assert by_name["Account"].included is True


def test_salesforce_distinguishes_custom_objects_and_reuses_object_pattern():
    result = list_salesforce_children(_config(), [], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["MyCustom__c"].kind == DatasetSubTypes.SALESFORCE_CUSTOM_OBJECT
    assert by_name["MyCustom__c"].included is True
    # The connector's own object_pattern deny (^tmp_) is reused for the verdict.
    assert by_name["tmp_scratch__c"].included is False
    assert by_name["tmp_scratch__c"].excluded_by == "object_pattern"
