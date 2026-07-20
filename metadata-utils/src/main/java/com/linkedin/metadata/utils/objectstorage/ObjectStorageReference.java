package com.linkedin.metadata.utils.objectstorage;

import javax.annotation.Nonnull;

public record ObjectStorageReference(@Nonnull String bucket, @Nonnull String key) {

  private static final String S3_PREFIX = "s3://";
  private static final String GCS_PREFIX = "gs://";

  public ObjectStorageReference {
    if (bucket == null || bucket.isBlank()) {
      throw new IllegalArgumentException("bucket must be non-empty");
    }
    if (key == null) {
      throw new IllegalArgumentException("key must not be null");
    }
  }

  @Nonnull
  public static ObjectStorageReference fromUri(@Nonnull String uri) {
    if (uri.startsWith(S3_PREFIX)) {
      return parseUri(uri.substring(S3_PREFIX.length()));
    }
    if (uri.startsWith(GCS_PREFIX)) {
      return parseUri(uri.substring(GCS_PREFIX.length()));
    }
    throw new IllegalArgumentException(
        "Unsupported object storage URI scheme (expected s3:// or gs://): " + uri);
  }

  @Nonnull
  private static ObjectStorageReference parseUri(@Nonnull String withoutScheme) {
    int slash = withoutScheme.indexOf('/');
    if (slash < 0) {
      return new ObjectStorageReference(withoutScheme, "");
    }
    String bucket = withoutScheme.substring(0, slash);
    String key = withoutScheme.substring(slash + 1);
    if (bucket.isEmpty()) {
      throw new IllegalArgumentException("bucket must be non-empty in URI");
    }
    return new ObjectStorageReference(bucket, key);
  }
}
