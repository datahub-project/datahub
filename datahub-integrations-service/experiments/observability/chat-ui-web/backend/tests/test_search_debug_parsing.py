"""Regression tests for search debug result parsing."""

import json

from api.routes.search_debug import _process_debug_result


def test_process_debug_result_preserves_zero_rescore_value() -> None:
    """rescoreValue=0.0 must not fall back to finalScore."""
    item = {
        "entity": {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)",
            "type": "DATASET",
            "name": "test",
            "properties": {"description": "desc"},
        },
        "score": 123.0,
        "matchedFields": [{"name": "name", "value": "test"}],
        "extraProperties": [
            {
                "name": "_rescoreExplain",
                "value": json.dumps(
                    {
                        "bm25Score": 10.0,
                        "rescoreValue": 0.0,
                        "finalScore": 9.5,
                        "rescoreBoost": 0.0,
                        "signals": {},
                    }
                ),
            }
        ],
    }

    result = _process_debug_result(
        item=item,
        rank=1,
        include_stage1=True,
        include_stage2=True,
    )

    assert result.scores.stage2RescoreValue == 0.0
    assert result.stage2Explanation is not None
    assert result.stage2Explanation.rescoreValue == 0.0
