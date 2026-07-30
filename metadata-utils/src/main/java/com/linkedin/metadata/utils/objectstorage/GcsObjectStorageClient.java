package com.linkedin.metadata.utils.objectstorage;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GcsObjectStorageClient implements ObjectStorageClient {

  public static final int DEFAULT_MULTIPART_THRESHOLD_BYTES = 8 * 1024 * 1024;
  public static final int DEFAULT_MULTIPART_PART_SIZE_BYTES = 8 * 1024 * 1024;

  @Nonnull private final Storage storage;
  @Nonnull private final String bucketName;
  @Nullable private final String pathPrefix;
  private final int multipartThresholdBytes;
  private final int multipartPartSizeBytes;

  public GcsObjectStorageClient(
      @Nonnull Storage storage, @Nonnull String bucketName, @Nullable String pathPrefix) {
    this(
        storage,
        bucketName,
        pathPrefix,
        DEFAULT_MULTIPART_THRESHOLD_BYTES,
        DEFAULT_MULTIPART_PART_SIZE_BYTES);
  }

  public GcsObjectStorageClient(
      @Nonnull Storage storage,
      @Nonnull String bucketName,
      @Nullable String pathPrefix,
      int multipartThresholdBytes,
      int multipartPartSizeBytes) {
    this.storage = storage;
    this.bucketName = bucketName;
    this.pathPrefix = pathPrefix;
    this.multipartThresholdBytes = multipartThresholdBytes;
    this.multipartPartSizeBytes = multipartPartSizeBytes;
  }

  @Override
  public void putObject(@Nonnull String objectKey, @Nonnull byte[] bytes) {
    if (!isConfigured()) {
      throw new IllegalStateException("GCS bucket name is not configured");
    }
    String key = ObjectStorageKeyResolver.joinKey(pathPrefix, objectKey, ObjectStorageProvider.GCS);
    BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(bucketName, key)).build();

    if (bytes.length < multipartThresholdBytes) {
      storage.create(blobInfo, bytes);
      log.debug("Uploaded {} bytes to gs://{}/{}", bytes.length, bucketName, key);
      return;
    }

    writeChunked(blobInfo, bytes);
    log.debug("Chunked uploaded {} bytes to gs://{}/{}", bytes.length, bucketName, key);
  }

  private void writeChunked(@Nonnull BlobInfo blobInfo, @Nonnull byte[] bytes) {
    try (WritableByteChannel channel = storage.writer(blobInfo)) {
      int offset = 0;
      while (offset < bytes.length) {
        int length = Math.min(multipartPartSizeBytes, bytes.length - offset);
        ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, length);
        while (buffer.hasRemaining()) {
          channel.write(buffer);
        }
        offset += length;
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to write object to GCS: " + e.getMessage(), e);
    }
  }

  @Override
  @Nonnull
  public ObjectStorageProvider provider() {
    return ObjectStorageProvider.GCS;
  }

  @Override
  public boolean isConfigured() {
    return bucketName != null && !bucketName.isBlank();
  }
}
