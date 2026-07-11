package com.linkedin.metadata.utils.objectstorage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class GcsObjectStorageClientTest {

  @Test
  public void testPutObjectSmallPayload() {
    Storage storage = mock(Storage.class);
    Blob blob = mock(Blob.class);
    when(storage.create(any(BlobInfo.class), any(byte[].class))).thenReturn(blob);

    GcsObjectStorageClient client =
        new GcsObjectStorageClient(storage, "my-bucket", "prefix", 1024, 512);
    client.putObject("exports/out.bin", new byte[] {9, 8, 7});

    ArgumentCaptor<BlobInfo> blobInfoCaptor = ArgumentCaptor.forClass(BlobInfo.class);
    verify(storage).create(blobInfoCaptor.capture(), eq(new byte[] {9, 8, 7}));
    assertEquals(blobInfoCaptor.getValue().getBucket(), "my-bucket");
    assertEquals(blobInfoCaptor.getValue().getName(), "prefix/exports/out.bin");
  }

  @Test
  public void testPutObjectLargePayloadUsesWriter() throws Exception {
    Storage storage = mock(Storage.class);
    WriteChannel channel = mock(WriteChannel.class);
    when(storage.writer(any(BlobInfo.class))).thenReturn(channel);
    when(channel.write(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              ByteBuffer buffer = invocation.getArgument(0);
              int remaining = buffer.remaining();
              buffer.position(buffer.limit());
              return remaining;
            });

    byte[] payload = new byte[1536];
    GcsObjectStorageClient client =
        new GcsObjectStorageClient(storage, "my-bucket", null, 1024, 1024);
    client.putObject("exports/large.bin", payload);

    verify(storage).writer(any(BlobInfo.class));
    verify(channel).close();
  }

  @Test
  public void testPutObjectLargePayloadWriteFailure() throws Exception {
    Storage storage = mock(Storage.class);
    WriteChannel channel = mock(WriteChannel.class);
    when(storage.writer(any(BlobInfo.class))).thenReturn(channel);
    when(channel.write(any(ByteBuffer.class))).thenThrow(new IOException("write failed"));

    GcsObjectStorageClient client =
        new GcsObjectStorageClient(storage, "my-bucket", null, 1024, 1024);
    try {
      client.putObject("exports/large.bin", new byte[1536]);
      throw new AssertionError("Expected RuntimeException");
    } catch (RuntimeException ex) {
      assertEquals(ex.getMessage(), "Failed to write object to GCS: write failed");
    }
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testPutObjectRejectsUnconfiguredBucket() {
    GcsObjectStorageClient client =
        new GcsObjectStorageClient(mock(Storage.class), "", null, 1024, 1024);
    client.putObject("key", new byte[] {1});
  }
}
