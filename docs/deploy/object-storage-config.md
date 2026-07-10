# Object storage configuration

Server-side object writes use `ObjectStorageClient` (`putObject`) with the same bucket env as file upload (`DATAHUB_BUCKET_NAME`) plus an optional path prefix or local filesystem root.

## Environment variables

| Variable                                   | Purpose                                                                               |
| ------------------------------------------ | ------------------------------------------------------------------------------------- |
| `DATAHUB_BUCKET_NAME`                      | S3 or GCS bucket (existing; from helm `global.storage.bucketPrefix`, scheme stripped) |
| `DATAHUB_OBJECT_STORAGE_PATH`              | Optional in-bucket prefix, or local filesystem root when bucket is unset              |
| `DATAHUB_OBJECT_STORAGE_PROVIDER`          | Optional hint: `s3`, `gcs`, or `local` (helm can derive from `bucketPrefix` scheme)   |
| `OBJECT_STORAGE_MULTIPART_THRESHOLD_BYTES` | Single-part vs multipart threshold (default 8 MiB)                                    |
| `OBJECT_STORAGE_MULTIPART_PART_SIZE_BYTES` | Multipart/chunk size (default 8 MiB)                                                  |

## Local development

```bash
# Filesystem root — leave bucket unset
DATAHUB_OBJECT_STORAGE_PATH=/tmp/datahub-object-storage
```

## Helm companion (datahub-helm-fork)

Add `global.storage.path` wired to `DATAHUB_OBJECT_STORAGE_PATH`. Derive `DATAHUB_OBJECT_STORAGE_PROVIDER` from `global.storage.bucketPrefix` (`gs://` → `gcs`, else `s3`). Existing `DATAHUB_BUCKET_NAME` wiring is unchanged.
