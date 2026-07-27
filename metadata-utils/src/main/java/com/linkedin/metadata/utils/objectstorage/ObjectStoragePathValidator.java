package com.linkedin.metadata.utils.objectstorage;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import javax.annotation.Nonnull;

public final class ObjectStoragePathValidator {

  private ObjectStoragePathValidator() {}

  @Nonnull
  public static Path resolveUnderRoot(@Nonnull Path baseRoot, @Nonnull String relativeKey) {
    String decodedKey = urlDecodeIfNeeded(relativeKey);
    validateRelativeKey(decodedKey);

    try {
      Path base = baseRoot.toRealPath(LinkOption.NOFOLLOW_LINKS);
      Path resolved = base.resolve(decodedKey).normalize();
      if (!resolved.startsWith(base)) {
        throw new ObjectStoragePathException("path escapes configured root");
      }
      return resolved;
    } catch (IOException e) {
      throw new ObjectStoragePathException(
          "unable to resolve object storage base path: " + e.getMessage());
    }
  }

  static void validateRelativeKey(@Nonnull String relativeKey) {
    if (relativeKey.isBlank()) {
      throw new ObjectStoragePathException("object key must be non-empty");
    }
    if (relativeKey.startsWith("/") || relativeKey.startsWith("\\")) {
      throw new ObjectStoragePathException("object key must be relative");
    }
    for (int i = 0; i < relativeKey.length(); i++) {
      if (Character.isISOControl(relativeKey.charAt(i))) {
        throw new ObjectStoragePathException("object key contains control characters");
      }
    }
    for (String segment : relativeKey.split("[/\\\\]")) {
      if ("..".equals(segment)) {
        throw new ObjectStoragePathException("object key must not contain '..' segments");
      }
    }
  }

  @Nonnull
  private static String urlDecodeIfNeeded(@Nonnull String key) {
    if (!key.contains("%")) {
      return key;
    }
    return URLDecoder.decode(key, StandardCharsets.UTF_8);
  }
}
