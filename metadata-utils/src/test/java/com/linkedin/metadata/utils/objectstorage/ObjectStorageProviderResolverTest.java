package com.linkedin.metadata.utils.objectstorage;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class ObjectStorageProviderResolverTest {

  @Test
  public void testExplicitProvider() {
    assertEquals(
        ObjectStorageProviderResolver.resolve("gcs", "my-bucket"), ObjectStorageProvider.GCS);
    assertEquals(
        ObjectStorageProviderResolver.resolve("s3", "my-bucket"), ObjectStorageProvider.S3);
    assertEquals(ObjectStorageProviderResolver.resolve("local", ""), ObjectStorageProvider.LOCAL);
  }

  @Test
  public void testEmptyBucketDefaultsToLocal() {
    assertEquals(ObjectStorageProviderResolver.resolve(null, null), ObjectStorageProvider.LOCAL);
    assertEquals(ObjectStorageProviderResolver.resolve("", ""), ObjectStorageProvider.LOCAL);
  }

  @Test
  public void testBucketWithoutProviderHintDefaultsToS3() {
    assertEquals(
        ObjectStorageProviderResolver.resolve(null, "my-bucket"), ObjectStorageProvider.S3);
  }
}
