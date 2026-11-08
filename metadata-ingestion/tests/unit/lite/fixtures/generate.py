"""Generate fixtures with acryl-datahub[datahub-lite]==1.7.0.19 and duckdb==1.5.6."""

import gzip
import tempfile
from importlib.metadata import version
from pathlib import Path

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.lite.lite_util import get_datahub_lite
from datahub.metadata.schema_classes import DatasetPropertiesClass, SystemMetadataClass


def main() -> None:
    for package, expected in (("acryl-datahub", "1.7.0.19"), ("duckdb", "1.5.6")):
        if version(package) != expected:
            raise RuntimeError(
                f"{package}=={expected} is required to generate fixtures"
            )
    for engine in ("sqlite", "duckdb"):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "catalog.db"
            lite = get_datahub_lite({"type": engine, "config": {"file": str(path)}})
            for name, observed in [("first", 1000), ("second", 2000), ("second", 3000)]:
                lite.write(
                    MetadataChangeProposalWrapper(
                        entityUrn="urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_schema.events,PROD)",
                        aspect=DatasetPropertiesClass(name=name),
                        systemMetadata=SystemMetadataClass(lastObserved=observed),
                    )
                )
            lite.close()
            (Path(__file__).parent / f"{engine}-1.7.0.19.db.gz").write_bytes(
                gzip.compress(path.read_bytes(), mtime=0)
            )


if __name__ == "__main__":
    main()
