package com.linkedin.metadata.utils.objectstorage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalObjectStorageClient implements ObjectStorageClient {

  @Nullable private final String rootPath;

  public LocalObjectStorageClient(@Nullable String rootPath) {
    this.rootPath = rootPath;
  }

  @Override
  public void putObject(@Nonnull String objectKey, @Nonnull byte[] bytes) {
    if (!isConfigured()) {
      throw new IllegalStateException("Local object storage root path is not configured");
    }
    String relativeKey =
        ObjectStorageKeyResolver.joinKey(null, objectKey, ObjectStorageProvider.LOCAL);
    Path target = ObjectStoragePathValidator.resolveUnderRoot(Path.of(rootPath), relativeKey);
    try {
      Files.createDirectories(target.getParent());
      Files.write(target, bytes);
      log.debug("Wrote {} bytes to {}", bytes.length, target);
    } catch (IOException e) {
      throw new RuntimeException("Failed to write object to local storage: " + e.getMessage(), e);
    }
  }

  @Override
  @Nonnull
  public ObjectStorageProvider provider() {
    return ObjectStorageProvider.LOCAL;
  }

  @Override
  public boolean isConfigured() {
    return rootPath != null && !rootPath.isBlank();
  }
}
