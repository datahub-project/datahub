package com.linkedin.metadata.utils.gcs;

import com.google.cloud.storage.Storage;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GcsUtil {

  private final Storage storage;

  public GcsUtil(@Nonnull Storage storage) {
    this.storage = storage;
  }

  /**
   * Read a GCS object's full body as a UTF-8 string. Intended for small objects (config / metadata
   * documents) fetched with the client's configured credentials — not for streaming large files.
   *
   * @param bucket The GCS bucket name
   * @param object The GCS object name
   * @return The object body decoded as UTF-8
   */
  public String getObjectAsString(@Nonnull String bucket, @Nonnull String object) {
    try {
      return new String(storage.readAllBytes(bucket, object), StandardCharsets.UTF_8);
    } catch (Exception e) {
      log.error("Failed to read object from GCS. Bucket: {}, Object: {}", bucket, object, e);
      throw new RuntimeException("Failed to read object from GCS: " + e.getMessage(), e);
    }
  }
}
