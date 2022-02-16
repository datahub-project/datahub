import json
from multiprocessing.pool import ThreadPool as Pool

import requests
from datahub.emitter.serialization_helper import pre_json_transform
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    BrowsePaths,
    Owner,
    Ownership,
    OwnershipType,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetProperties
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot


def _build_snapshot(id: int):
    urn = _build_urn(id)
    return DatasetSnapshot(
        urn,
        [
            _build_properties(),
            _build_ownership(id),
            _build_browsepaths(id),
        ],
    )


def _build_urn(id: int):
    return f"urn:li:dataset:(urn:li:dataPlatform:bigquery,test_dataset_{id},PROD)"


def _build_properties():
    return DatasetProperties(description="This is a great dataset")


def _build_browsepaths(id: int):
    return BrowsePaths([f"/perf/testing/path/{id}"])


def _build_ownership(id: int):
    return Ownership(
        [
            Owner(f"urn:li:corpuser:test_{id}", OwnershipType.DATAOWNER),
            Owner("urn:li:corpuser:common", OwnershipType.DATAOWNER),
        ]
    )


def main(url: str, id: int):
    proposed_snapshot = _build_snapshot(id)
    snapshot_fqn = (
        f"com.linkedin.metadata.snapshot.{proposed_snapshot.RECORD_SCHEMA.name}"
    )
    requests.post(
        f"{url}/entities?action=ingest",
        data=json.dumps(
            {
                "entity": {
                    "value": {
                        snapshot_fqn: pre_json_transform(proposed_snapshot.to_obj())
                    }
                }
            }
        ),
    )


def worker(index: int):
    try:
        main("http://localhost:8080", index)
    except RuntimeError as e:
        print(f"error with {index}")


if __name__ == "__main__":

    POOL_SIZE = 10
    DATASETS = 100000

    pool = Pool(POOL_SIZE)
    for i in range(1, DATASETS + 1):
        pool.apply_async(worker, (i,))
    pool.close()
    pool.join()
