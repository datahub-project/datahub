package com.linkedin.metadata.utils.objectstorage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface ObjectStorageClient {

  void putObject(@Nonnull String objectKey, @Nonnull byte[] bytes);

  @Nonnull
  default String presignedDownloadUrl(@Nonnull ObjectStorageReference ref, int expirationSeconds) {
    throw unsupported("presignedDownloadUrl");
  }

  @Nonnull
  default String presignedUploadUrl(
      @Nonnull ObjectStorageReference ref, int expirationSeconds, @Nullable String contentType) {
    throw unsupported("presignedUploadUrl");
  }

  default void deleteObject(@Nonnull ObjectStorageReference ref) {
    throw unsupported("deleteObject");
  }

  @Nonnull
  ObjectStorageProvider provider();

  boolean isConfigured();

  @Nonnull
  private UnsupportedObjectStorageOperation unsupported(@Nonnull String operation) {
    return new UnsupportedObjectStorageOperation(
        operation + " not supported for provider " + provider());
  }
}
