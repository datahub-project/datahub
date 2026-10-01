#!/usr/bin/env python3
"""Load a lineage-bench dataset into DataHub's MySQL metadata_aspect_v2 (PLAN.md §1.3).

Deletes only rows under the bench URN prefix, then bulk-loads datasetKey, status and
upstreamLineage aspects with LOAD DATA LOCAL INFILE via the mysql client inside the container.
Run LoadIndices afterwards to build the OpenSearch indices. Stdlib only.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Sequence

NOW = "2026-01-01 00:00:00.000000"
ACTOR = "urn:li:corpuser:datahub"
SYSTEM_METADATA = json.dumps({"runId": "lineage-bench", "lastObserved": 0})


def mysql(container: str, sql: str, local_infile: bool = False) -> str:
    cmd = ["docker", "exec", "-i", container, "mysql", "-uroot", "-pdatahub", "datahub", "-N", "-e", sql]
    if local_infile:
        cmd.insert(5, "--local-infile=1")
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"mysql failed: {res.stderr.strip()}")
    return res.stdout.strip()


def find_container() -> str:
    out = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}", "--filter", "name=mysql"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    if len(out) != 1:
        raise RuntimeError(f"expected exactly one running mysql container, found {out}; pass --container")
    return out[0]


def write_tsv(dataset: Path, tsv: Path) -> int:
    upstreams: Dict[str, List[str]] = defaultdict(list)
    with (dataset / "edges.csv").open() as f:
        for row in csv.DictReader(f):
            upstreams[row["downstream_urn"]].append(row["upstream_urn"])

    rows = 0
    with (dataset / "nodes.csv").open() as f, tsv.open("w") as out:

        def emit(urn: str, aspect: str, value: dict) -> None:
            nonlocal rows
            out.write("\t".join([urn, aspect, "0", json.dumps(value), SYSTEM_METADATA, NOW, ACTOR]) + "\n")
            rows += 1

        for row in csv.DictReader(f):
            urn = row["urn"]
            # urn:li:dataset:(urn:li:dataPlatform:snowflake,<name>,PROD)
            name = urn[len("urn:li:dataset:(urn:li:dataPlatform:snowflake,") : -len(",PROD)")]
            emit(urn, "datasetKey", {"platform": "urn:li:dataPlatform:snowflake", "name": name, "origin": "PROD"})
            emit(urn, "status", {"removed": False})
            ups = upstreams.get(urn)
            if ups:
                emit(
                    urn,
                    "upstreamLineage",
                    {
                        "upstreams": [
                            {"auditStamp": {"time": 0, "actor": ACTOR}, "dataset": u, "type": "TRANSFORMED"}
                            for u in ups
                        ]
                    },
                )
    return rows


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("dataset", type=Path)
    ap.add_argument("--container", default=None, help="mysql container name (auto-detected)")
    args = ap.parse_args(argv)

    manifest = json.loads((args.dataset / "manifest.json").read_text())
    prefix = manifest["urn_prefix"]
    container = args.container or find_container()

    with tempfile.TemporaryDirectory() as tmp:
        tsv = Path(tmp) / "aspects.tsv"
        rows = write_tsv(args.dataset, tsv)
        print(f"wrote {rows:,} aspect rows", file=sys.stderr)
        subprocess.run(["docker", "cp", str(tsv), f"{container}:/tmp/aspects.tsv"], check=True)

    like = prefix.replace("_", "\\\\_") + "%"
    mysql(container, "SET GLOBAL local_infile=1;")
    mysql(container, f"DELETE FROM metadata_aspect_v2 WHERE urn LIKE '{like}';")
    mysql(
        container,
        "LOAD DATA LOCAL INFILE '/tmp/aspects.tsv' INTO TABLE metadata_aspect_v2 "
        "CHARACTER SET utf8mb4 FIELDS TERMINATED BY '\\t' ESCAPED BY '' LINES TERMINATED BY '\\n' "
        "(urn, aspect, version, metadata, systemmetadata, createdon, createdby);",
        local_infile=True,
    )
    subprocess.run(["docker", "exec", container, "rm", "-f", "/tmp/aspects.tsv"], check=True)

    loaded = int(mysql(container, f"SELECT COUNT(*) FROM metadata_aspect_v2 WHERE urn LIKE '{like}';"))
    print(f"metadata_aspect_v2 bench rows: {loaded:,} (expected {rows:,})")
    return 0 if loaded == rows else 1


if __name__ == "__main__":
    sys.exit(main())
