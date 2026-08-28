"""Seed N cloned dbt artifact sets into S3 or a local directory.

Used to scale-test multi-project (glob manifest_path) dbt ingestion:

    python -m tests.performance.dbt.seed_projects --target s3://bucket/prefix --count 1000
    python -m tests.performance.dbt.seed_projects --target s3://bucket/prefix --clean
"""

import argparse
import logging
import shutil
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Iterator, Optional, Tuple
from urllib.parse import urlparse

import boto3

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

logger = logging.getLogger(__name__)

# Checked-in single-project artifacts used as the clone template. One raw-text
# replacement of the "jaffle_shop" token per clone uniquifies both cross-project
# collision dimensions at once: the dbt package name embedded in every unique_id
# (model.jaffle_shop.customers -> model.jaffle_shop_00042.customers) and the
# target schema (jaffle_shop -> jaffle_shop_00042), so N clones ingest as N
# independent, non-colliding dbt projects.
_TEMPLATE_DIR = Path(__file__).parents[2] / "integration" / "dbt"
_PROJECT_TOKEN = "jaffle_shop"
_TEMPLATE_FILES: Dict[str, str] = {
    "manifest.json": "jaffle_shop_manifest.json",
    "catalog.json": "jaffle_shop_catalog.json",
}
_RUN_RESULTS_FILE: Tuple[str, str] = (
    "run_results.json",
    "jaffle_shop_test_results.json",
)


def project_name(index: int) -> str:
    return f"{_PROJECT_TOKEN}_{index:05d}"


def _load_templates(include_run_results: bool) -> Dict[str, str]:
    files = dict(_TEMPLATE_FILES)
    if include_run_results:
        files[_RUN_RESULTS_FILE[0]] = _RUN_RESULTS_FILE[1]
    return {
        name: (_TEMPLATE_DIR / source).read_text() for name, source in files.items()
    }


def _iter_objects(templates: Dict[str, str], count: int) -> Iterator[Tuple[str, str]]:
    """Yield (relative path, rendered content) for every file of every clone."""
    for index in range(count):
        name = project_name(index)
        for filename, text in templates.items():
            yield f"{name}/{filename}", text.replace(_PROJECT_TOKEN, name)


def _make_s3_client(profile: Optional[str]) -> "S3Client":
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    return session.client("s3")


def _parse_s3_uri(uri: str) -> Tuple[str, str]:
    parsed = urlparse(uri)
    return parsed.netloc, parsed.path.strip("/")


def seed(
    target: str,
    count: int,
    *,
    include_run_results: bool = False,
    profile: Optional[str] = None,
    max_workers: int = 16,
) -> None:
    """Write count cloned projects to target/<project_name>/<artifact>.json."""
    templates = _load_templates(include_run_results)
    if target.startswith("s3://"):
        bucket, prefix = _parse_s3_uri(target)
        client = _make_s3_client(profile)
        objects = _iter_objects(templates, count)
        uploaded = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit in bounded batches: rendering is lazy, so this caps rendered
            # manifest bodies in memory at ~one batch instead of count * files.
            while batch := list(islice(objects, max_workers * 4)):
                futures = [
                    executor.submit(
                        client.put_object,
                        Bucket=bucket,
                        Key=f"{prefix}/{path}" if prefix else path,
                        Body=content.encode("utf-8"),
                    )
                    for path, content in batch
                ]
                for future in futures:
                    future.result()
                uploaded += len(batch)
                logger.info(f"Uploaded {uploaded} objects to s3://{bucket}/{prefix}")
    else:
        root = Path(target)
        for path, content in _iter_objects(templates, count):
            file_path = root / path
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(content)
    logger.info(
        f"Seeded {count} projects ({count * len(templates)} files) into {target}"
    )


def clean(target: str, *, profile: Optional[str] = None) -> None:
    """Delete everything under target (an S3 prefix or a local directory)."""
    if target.startswith("s3://"):
        bucket, prefix = _parse_s3_uri(target)
        client = _make_s3_client(profile)
        paginator = client.get_paginator("list_objects_v2")
        deleted = 0
        for page in paginator.paginate(
            Bucket=bucket, Prefix=f"{prefix}/" if prefix else ""
        ):
            keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                deleted += len(keys)
        logger.info(f"Deleted {deleted} objects under {target}")
    else:
        shutil.rmtree(target, ignore_errors=True)
        logger.info(f"Removed {target}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target", required=True, help="s3://bucket/prefix or a local directory"
    )
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument(
        "--run-results",
        action="store_true",
        help="also seed a run_results.json per project",
    )
    parser.add_argument(
        "--clean", action="store_true", help="delete everything under target and exit"
    )
    parser.add_argument("--profile", help="AWS profile name for boto3")
    parser.add_argument("--workers", type=int, default=16)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    if args.clean:
        clean(args.target, profile=args.profile)
        return
    seed(
        args.target,
        args.count,
        include_run_results=args.run_results,
        profile=args.profile,
        max_workers=args.workers,
    )


if __name__ == "__main__":
    main()
