package com.linkedin.metadata.utils.objectstorage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

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
  public void testDeleteObjectUnsupported() {
    ObjectStorageReference ref = new ObjectStorageReference("bucket", "key");
    try {
      client.deleteObject(ref);
      fail("Expected UnsupportedObjectStorageOperation");
    } catch (UnsupportedObjectStorageOperation ex) {
      assertEquals(ex.getMessage(), "deleteObject not supported for provider LOCAL");
    }
  }
}
