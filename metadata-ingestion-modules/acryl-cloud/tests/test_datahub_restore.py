from acryl_datahub_cloud.datahub_restore.do_restore import _parse_response


def test_parse_args() -> None:
    response = {
        "value": "{args=RestoreIndicesArgs(start=0, batchSize=100, limit=0, numThreads=1, batchDelayMs=1000, gePitEpochMs=0, lePitEpochMs=1738171914414, aspectName=null, aspectNames=[], urn=urn:li:container:00574c9e83ccb045d2d5039f4b1ede9a, urnLike=null, urnBasedPagination=false, lastUrn=, lastAspect=), result=RestoreIndicesResult(ignored=0, rowsMigrated=8, timeSqlQueryMs=2, timeGetRowMs=4, timeUrnMs=0, timeEntityRegistryCheckMs=0, aspectCheckMs=0, createRecordMs=0, sendMessageMs=37, defaultAspectsCreated=0, lastUrn=urn:li:container:00574c9e83ccb045d2d5039f4b1ede9a, lastAspect=testResults)}",
    }
    assert _parse_response(response) == {
        "args": {
            "start": 0,
            "aspectName": None,
            "aspectNames": [],
            "batchDelayMs": 1000,
            "batchSize": 100,
            "gePitEpochMs": 0,
            "lastAspect": "",
            "lastUrn": "",
            "lePitEpochMs": 1738171914414,
            "limit": 0,
            "numThreads": 1,
            "urn": "urn:li:container:00574c9e83ccb045d2d5039f4b1ede9a",
            "urnBasedPagination": False,
            "urnLike": None,
        },
        "result": {
            "aspectCheckMs": 0,
            "createRecordMs": 0,
            "defaultAspectsCreated": 0,
            "ignored": 0,
            "lastAspect": "testResults",
            "lastUrn": "urn:li:container:00574c9e83ccb045d2d5039f4b1ede9a",
            "rowsMigrated": 8,
            "sendMessageMs": 37,
            "timeEntityRegistryCheckMs": 0,
            "timeGetRowMs": 4,
            "timeSqlQueryMs": 2,
            "timeUrnMs": 0,
        },
    }
