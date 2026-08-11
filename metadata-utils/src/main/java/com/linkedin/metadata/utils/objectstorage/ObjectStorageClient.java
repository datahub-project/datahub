package com.linkedin.metadata.utils.objectstorage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface ObjectStorageClient {

  void putObject(@Nonnull String objectKey, @Nonnull byte[] bytes);

  /**
   * Read an object's full body as a UTF-8 string. Intended for small documents (config / metadata)
   * fetched with the client's configured credentials — not for streaming large files.
   *
   * <p>{@code objectKey} is resolved against the client's configured path prefix, exactly as in
   * {@link #putObject}.
   */
  @Nonnull
  default String getObjectAsString(@Nonnull String objectKey) {
    throw unsupported("getObjectAsString");
  }

  default boolean supportsPresignedUrls() {
    return false;
  }

  @Nullable
  default String storageBucket() {
    return null;
  }

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
