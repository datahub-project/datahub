package com.linkedin.metadata.utils.objectstorage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class ObjectStorageProviderResolver {

  private ObjectStorageProviderResolver() {}

  @Nonnull
  public static ObjectStorageProvider resolve(
      @Nullable String configuredProvider, @Nullable String bucketName) {
    ObjectStorageProvider explicit = parseProvider(configuredProvider);
    if (explicit != null) {
      return explicit;
    }

    if (bucketName == null || bucketName.isBlank()) {
      return ObjectStorageProvider.LOCAL;
    }

    return ObjectStorageProvider.S3;
  }

  @Nullable
  private static ObjectStorageProvider parseProvider(@Nullable String configuredProvider) {
    if (configuredProvider == null || configuredProvider.isBlank()) {
      return null;
    }
    return switch (configuredProvider.trim().toLowerCase()) {
      case "s3" -> ObjectStorageProvider.S3;
      case "gcs" -> ObjectStorageProvider.GCS;
      case "local" -> ObjectStorageProvider.LOCAL;
      default -> throw new IllegalArgumentException(
          "Unsupported object storage provider: " + configuredProvider);
    };
  }
}
