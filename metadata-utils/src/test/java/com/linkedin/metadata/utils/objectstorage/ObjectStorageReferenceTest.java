package com.linkedin.metadata.utils.objectstorage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import org.testng.annotations.Test;

public class ObjectStorageReferenceTest {

  @Test
  public void testFromUriS3() {
    ObjectStorageReference ref =
        ObjectStorageReference.fromUri("s3://my-bucket/exports/2026/report.bin");
    assertEquals(ref.bucket(), "my-bucket");
    assertEquals(ref.key(), "exports/2026/report.bin");
  }

  @Test
  public void testFromUriGcs() {
    ObjectStorageReference ref = ObjectStorageReference.fromUri("gs://my-bucket/path/to/object");
    assertEquals(ref.bucket(), "my-bucket");
    assertEquals(ref.key(), "path/to/object");
  }

  @Test
  public void testFromUriBucketOnly() {
    ObjectStorageReference ref = ObjectStorageReference.fromUri("s3://my-bucket");
    assertEquals(ref.bucket(), "my-bucket");
    assertEquals(ref.key(), "");
  }

  @Test
  public void testFromUriUnsupportedScheme() {
    assertThrows(
        IllegalArgumentException.class, () -> ObjectStorageReference.fromUri("file:///tmp"));
  }
}
