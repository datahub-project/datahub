package com.linkedin.metadata.utils.objectstorage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class ObjectStorageKeyResolver {

  private ObjectStorageKeyResolver() {}

  /**
   * Join optional infra {@code pathPrefix} with caller {@code objectKey}. For cloud providers the
   * result is an object key inside the bucket. For {@link ObjectStorageProvider#LOCAL}, {@code
   * pathPrefix} is the filesystem root and validation uses {@link ObjectStoragePathValidator}.
   */
  @Nonnull
  public static String joinKey(
      @Nullable String pathPrefix,
      @Nonnull String objectKey,
      @Nonnull ObjectStorageProvider provider) {
    String normalizedKey = normalizeObjectKey(objectKey);
    if (pathPrefix == null || pathPrefix.isBlank()) {
      if (provider == ObjectStorageProvider.LOCAL) {
        ObjectStoragePathValidator.validateRelativeKey(normalizedKey);
      }
      return normalizedKey;
    }

    String normalizedPrefix = normalizePrefix(pathPrefix);
    String joined = normalizedPrefix + "/" + normalizedKey;
    if (provider == ObjectStorageProvider.LOCAL) {
      ObjectStoragePathValidator.validateRelativeKey(joined);
    }
    return joined;
  }

  @Nonnull
  private static String normalizeObjectKey(@Nonnull String objectKey) {
    String trimmed = objectKey.trim();
    while (trimmed.startsWith("/")) {
      trimmed = trimmed.substring(1);
    }
    return trimmed;
  }

  @Nonnull
  private static String normalizePrefix(@Nonnull String pathPrefix) {
    String trimmed = pathPrefix.trim();
    while (trimmed.endsWith("/")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    while (trimmed.startsWith("/")) {
      trimmed = trimmed.substring(1);
    }
    return trimmed;
  }
}
