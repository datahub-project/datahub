package com.linkedin.metadata.utils.objectstorage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;

import java.nio.file.Files;
import java.nio.file.Path;
import org.testng.annotations.Test;

public class ObjectStorageClientTest {

  private final ObjectStorageClient client =
      new LocalObjectStorageClient(System.getProperty("java.io.tmpdir"));

  @Test
  public void testPresignedDownloadUrlUnsupported() {
    ObjectStorageReference ref = new ObjectStorageReference("bucket", "key");
    try {
      client.presignedDownloadUrl(ref, 3600);
      fail("Expected UnsupportedObjectStorageOperation");
    } catch (UnsupportedObjectStorageOperation ex) {
      assertEquals(ex.getMessage(), "presignedDownloadUrl not supported for provider LOCAL");
    }
  }

  @Test
  public void testPresignedUploadUrlUnsupported() {
    ObjectStorageReference ref = new ObjectStorageReference("bucket", "key");
    try {
      client.presignedUploadUrl(ref, 3600, "application/octet-stream");
      fail("Expected UnsupportedObjectStorageOperation");
    } catch (UnsupportedObjectStorageOperation ex) {
      assertEquals(ex.getMessage(), "presignedUploadUrl not supported for provider LOCAL");
    }
  }

  @Test
  public void testDeleteObjectRemovesLocalFile() throws Exception {
    Path root = Files.createTempDirectory("object-storage-test");
    ObjectStorageClient localClient = new LocalObjectStorageClient(root.toString());
    localClient.putObject("test/key", new byte[] {1});
    Path file = root.resolve("test/key");
    assertEquals(Files.exists(file), true);

    localClient.deleteObject(new ObjectStorageReference("bucket", "test/key"));
    assertFalse(Files.exists(file));
  }
}
