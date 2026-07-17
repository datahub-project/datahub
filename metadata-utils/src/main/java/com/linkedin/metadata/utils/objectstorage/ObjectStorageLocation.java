package com.linkedin.metadata.utils.objectstorage;

import java.net.URI;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Parsed object-storage root location. Cloud URIs use {@code s3://bucket} or {@code
 * s3://bucket/optional-prefix} (same for {@code gs://}); local uses {@code file:///absolute/path}.
 * A cloud URI without a path segment addresses the bucket root.
 */
public record ObjectStorageLocation(
    @Nonnull ObjectStorageProvider provider,
    @Nullable String bucket,
    @Nullable String keyPrefix,
    @Nullable String localRoot) {

  private static final String S3_SCHEME = "s3";
  private static final String GCS_SCHEME = "gs";
  private static final String FILE_SCHEME = "file";

  public ObjectStorageLocation {
    switch (provider) {
      case LOCAL -> {
        if (localRoot == null || localRoot.isBlank()) {
          throw new IllegalArgumentException("local object storage requires a non-empty root path");
        }
      }
      case S3, GCS -> {
        if (bucket == null || bucket.isBlank()) {
          throw new IllegalArgumentException(
              provider + " object storage requires a non-empty bucket");
        }
      }
      default -> throw new IllegalStateException("Unexpected provider: " + provider);
    }
  }

  /**
   * Resolves a location from {@code DATAHUB_OBJECT_STORAGE_URI} when set, otherwise synthesizes
   * from legacy bucket/path/provider env vars.
   */
  @Nonnull
  public static Optional<ObjectStorageLocation> resolve(
      @Nullable String configuredUri,
      @Nullable String legacyBucketName,
      @Nullable String legacyPath,
      @Nullable String legacyProvider) {
    if (configuredUri != null && !configuredUri.isBlank()) {
      return Optional.of(parse(configuredUri.trim()));
    }
    return synthesizeFromLegacy(legacyBucketName, legacyPath, legacyProvider);
  }

  @Nonnull
  public static ObjectStorageLocation parse(@Nonnull String uri) {
    String trimmed = uri.trim();
    if (trimmed.startsWith(S3_SCHEME + "://")) {
      return parseCloudUri(
          ObjectStorageProvider.S3, trimmed.substring((S3_SCHEME + "://").length()));
    }
    if (trimmed.startsWith(GCS_SCHEME + "://")) {
      return parseCloudUri(
          ObjectStorageProvider.GCS, trimmed.substring((GCS_SCHEME + "://").length()));
    }
    if (trimmed.startsWith(FILE_SCHEME + "://") || trimmed.startsWith(FILE_SCHEME + ":")) {
      return parseFileUri(trimmed);
    }
    throw new IllegalArgumentException(
        "Unsupported object storage URI scheme (expected s3://, gs://, or file://): " + uri);
  }

  @Nonnull
  private static Optional<ObjectStorageLocation> synthesizeFromLegacy(
      @Nullable String legacyBucketName,
      @Nullable String legacyPath,
      @Nullable String legacyProvider) {
    if (legacyBucketName != null && !legacyBucketName.isBlank()) {
      String trimmedBucket = legacyBucketName.trim();
      if (trimmedBucket.startsWith(S3_SCHEME + "://")
          || trimmedBucket.startsWith(GCS_SCHEME + "://")) {
        return Optional.of(parse(joinCloudUri(trimmedBucket, legacyPath)));
      }
    }

    ObjectStorageProvider provider =
        ObjectStorageProviderResolver.resolve(legacyProvider, legacyBucketName);

    return switch (provider) {
      case LOCAL -> synthesizeLocal(legacyPath);
      case S3 -> synthesizeCloud(S3_SCHEME, legacyBucketName, legacyPath);
      case GCS -> synthesizeCloud(GCS_SCHEME, legacyBucketName, legacyPath);
    };
  }

  @Nonnull
  private static Optional<ObjectStorageLocation> synthesizeLocal(@Nullable String legacyPath) {
    if (legacyPath == null || legacyPath.isBlank()) {
      return Optional.empty();
    }
    return Optional.of(parse(toFileUri(legacyPath)));
  }

  @Nonnull
  private static Optional<ObjectStorageLocation> synthesizeCloud(
      @Nonnull String scheme, @Nullable String legacyBucketName, @Nullable String legacyPath) {
    if (legacyBucketName == null || legacyBucketName.isBlank()) {
      return Optional.empty();
    }
    return Optional.of(parse(buildCloudUri(scheme, legacyBucketName, legacyPath)));
  }

  @Nonnull
  private static ObjectStorageLocation parseCloudUri(
      @Nonnull ObjectStorageProvider provider, @Nonnull String withoutScheme) {
    int slash = withoutScheme.indexOf('/');
    if (slash < 0) {
      return new ObjectStorageLocation(provider, withoutScheme, "", null);
    }
    String bucket = withoutScheme.substring(0, slash);
    String prefix = withoutScheme.substring(slash + 1);
    if (bucket.isEmpty()) {
      throw new IllegalArgumentException("bucket must be non-empty in URI");
    }
    return new ObjectStorageLocation(provider, bucket, normalizePrefix(prefix), null);
  }

  @Nonnull
  private static ObjectStorageLocation parseFileUri(@Nonnull String uri) {
    URI parsed = URI.create(uri);
    if (!FILE_SCHEME.equalsIgnoreCase(parsed.getScheme())) {
      throw new IllegalArgumentException("Unsupported file URI: " + uri);
    }
    String path = parsed.getPath();
    if (path == null || path.isBlank()) {
      throw new IllegalArgumentException("file URI must include a path: " + uri);
    }
    return new ObjectStorageLocation(ObjectStorageProvider.LOCAL, null, null, path);
  }

  @Nonnull
  private static String buildCloudUri(
      @Nonnull String scheme, @Nonnull String bucketName, @Nullable String path) {
    return joinCloudUri(scheme + "://" + bucketName.trim(), path);
  }

  @Nonnull
  private static String joinCloudUri(@Nonnull String bucketUri, @Nullable String path) {
    String normalizedPath = normalizePrefix(path);
    if (normalizedPath.isEmpty()) {
      return bucketUri;
    }
    String base =
        bucketUri.endsWith("/") ? bucketUri.substring(0, bucketUri.length() - 1) : bucketUri;
    return base + "/" + normalizedPath;
  }

  @Nonnull
  private static String toFileUri(@Nonnull String filesystemPath) {
    String trimmed = filesystemPath.trim();
    if (trimmed.startsWith(FILE_SCHEME + "://") || trimmed.startsWith(FILE_SCHEME + ":")) {
      return trimmed;
    }
    if (!trimmed.startsWith("/")) {
      throw new IllegalArgumentException(
          "Local object storage path must be absolute (or use file:// URI): " + filesystemPath);
    }
    return FILE_SCHEME + "://" + trimmed;
  }

  @Nonnull
  private static String normalizePrefix(@Nullable String path) {
    if (path == null || path.isBlank()) {
      return "";
    }
    String trimmed = path.trim();
    while (trimmed.startsWith("/")) {
      trimmed = trimmed.substring(1);
    }
    while (trimmed.endsWith("/")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    return trimmed;
  }
}
