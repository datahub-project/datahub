"""
This script is designed to manage and clean up contents in an S3 bucket, specifically targeting orphaned files and folders.
It provides functionality to list, delete, or simulate deletion of all objects under a specified S3 prefix using AWS assumed role credentials.

The script supports the following operations:
- Listing all files and folders under a specified S3 path.
- Deleting all contents under a specified S3 path.
- Performing a dry run to show what would be deleted without actually deleting the objects.

Environment variables required:
- DH_ICEBERG_AWS_ROLE: The ARN of the AWS role to assume.
- DH_ICEBERG_CLIENT_ID: The AWS client ID.
- DH_ICEBERG_CLIENT_SECRET: The AWS client secret.

Usage:
    python folder_operations.py s3://bucket/prefix --list
    python folder_operations.py s3://bucket/prefix --nuke
    python folder_operations.py s3://bucket/prefix --dry-run

Arguments:
- s3_path: The S3 path to operate on (e.g., s3://bucket/prefix).
- --list: List all folders and files.
- --nuke: Delete all contents.
- --dry-run: Show what would be deleted without actually deleting.
- --region: AWS region (default: us-east-1).

Note: Only one action (--list, --nuke, or --dry-run) can be specified at a time.

"""

import argparse
import os
from datetime import datetime
from typing import Optional, Tuple

import boto3
from mypy_boto3_s3 import S3Client


def get_s3_client_with_role(
    client_id: str,
    client_secret: str,
    role_arn: str,
    region: str = "us-east-1",
    session_name: str = "IcebergSession",
) -> Tuple[S3Client, datetime]:  # type: ignore
    """
    Create an S3 client with assumed role credentials.
    """
    session = boto3.Session(
        aws_access_key_id=client_id,
        aws_secret_access_key=client_secret,
        region_name=region,
    )

    sts_client = session.client("sts")

    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn, RoleSessionName=session_name
    )

    credentials = assumed_role_object["Credentials"]

    s3_client: S3Client = boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )

    return s3_client, credentials["Expiration"]


def delete_s3_objects(
    s3_client: S3Client, bucket_name: str, prefix: str, dry_run: bool = False
) -> None:
    """
    Delete all objects under the specified prefix.
    """
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        objects_to_delete = []
        for obj in page.get("Contents", []):
            objects_to_delete.append({"Key": obj["Key"]})
            if dry_run:
                print(f"Would delete: {obj['Key']}")
                print(f"   Size: {obj['Size'] / (1024 * 1024):.2f} MB")
                print(f"   Last Modified: {obj['LastModified']}")

        if objects_to_delete and not dry_run:
            s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={"Objects": objects_to_delete},  # type: ignore
            )
            print(f"Deleted {len(objects_to_delete)} objects")


def list_s3_contents(
    s3_path: str,
    client_id: str,
    client_secret: str,
    role_arn: str,
    region: str = "us-east-1",
    delimiter: Optional[str] = None,
    nuke: bool = False,
    dry_run: bool = False,
) -> None:
    """
    List or delete contents of an S3 path using assumed role credentials.
    """
    if not s3_path.startswith("s3://"):
        raise ValueError("S3 path must start with 's3://'")

    bucket_name = s3_path.split("/")[2]
    prefix = "/".join(s3_path.split("/")[3:])
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    s3_client, expiration = get_s3_client_with_role(
        client_id=client_id,
        client_secret=client_secret,
        role_arn=role_arn,
        region=region,
    )

    operation = "Deleting" if nuke else "Would delete" if dry_run else "Listing"
    print(f"\n{operation} contents of {s3_path}")
    print(f"Using role: {role_arn}")
    print(f"Credentials expire at: {expiration}")
    print("-" * 60)

    if nuke or dry_run:
        delete_s3_objects(s3_client, bucket_name, prefix, dry_run)
        return

    paginator = s3_client.get_paginator("list_objects_v2")

    list_params = {"Bucket": bucket_name, "Prefix": prefix}
    if delimiter:
        list_params["Delimiter"] = delimiter

    try:
        pages = paginator.paginate(**list_params)  # type: ignore
        found_contents = False

        for page in pages:
            if delimiter and "CommonPrefixes" in page:
                for common_prefix in page.get("CommonPrefixes", []):
                    found_contents = True
                    folder_name = common_prefix["Prefix"][len(prefix) :].rstrip("/")
                    print(f"📁 {folder_name}/")

            for obj in page.get("Contents", []):
                found_contents = True
                file_path = obj["Key"][len(prefix) :]
                if file_path:
                    size_mb = obj["Size"] / (1024 * 1024)
                    print(f"📄 {file_path}")
                    print(f"   Size: {size_mb:.2f} MB")
                    print(f"   Last Modified: {obj['LastModified']}")

        if not found_contents:
            print("No contents found in the specified path.")

    except Exception as e:
        print(f"Error accessing contents: {str(e)}")


def main():
    parser = argparse.ArgumentParser(description="S3 Content Manager")
    parser.add_argument(
        "s3_path", help="S3 path to operate on (e.g., s3://bucket/prefix)"
    )
    parser.add_argument(
        "--list", action="store_true", help="List all folders and files"
    )
    parser.add_argument("--nuke", action="store_true", help="Delete all contents")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without actually deleting",
    )
    parser.add_argument(
        "--region", default="us-east-1", help="AWS region (default: us-east-1)"
    )

    args = parser.parse_args()

    # Get environment variables
    role_arn = os.environ.get("DH_ICEBERG_AWS_ROLE")
    client_id = os.environ.get("DH_ICEBERG_CLIENT_ID")
    client_secret = os.environ.get("DH_ICEBERG_CLIENT_SECRET")

    if not all([role_arn, client_id, client_secret]):
        raise ValueError(
            "Missing required environment variables. Please set DH_ICEBERG_AWS_ROLE, DH_ICEBERG_CLIENT_ID, and DH_ICEBERG_CLIENT_SECRET"
        )

    # Validate arguments
    if sum([args.list, args.nuke, args.dry_run]) != 1:
        parser.error("Please specify exactly one action: --list, --nuke, or --dry-run")

    list_s3_contents(
        args.s3_path,
        client_id=client_id,  # type: ignore
        client_secret=client_secret,  # type: ignore
        role_arn=role_arn,  # type: ignore
        region=args.region,
        # delimiter='/' if args.list else None,
        nuke=args.nuke,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
