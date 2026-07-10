package com.linkedin.metadata.utils.objectstorage;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ObjectStorageProviderResolverTest {

  private String previousGcsCredentials;

  @BeforeMethod
  public void saveEnv() {
    previousGcsCredentials = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
  }

  @AfterMethod
  public void restoreEnv() {
    if (previousGcsCredentials == null) {
      // Cannot unset env in Java; tests below use explicit provider hints when needed.
    }
  }

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
  public void testBucketWithoutHintDefaultsToS3WhenNoGcsEnv() {
    if (previousGcsCredentials != null && !previousGcsCredentials.isBlank()) {
      return;
    }
    assertEquals(
        ObjectStorageProviderResolver.resolve(null, "my-bucket"), ObjectStorageProvider.S3);
  }
}
