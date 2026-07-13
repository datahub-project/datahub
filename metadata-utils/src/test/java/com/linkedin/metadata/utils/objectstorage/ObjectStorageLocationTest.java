package com.linkedin.metadata.utils.objectstorage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;

import org.testng.annotations.Test;

public class ObjectStorageLocationTest {

  @Test
  public void testParseS3WithPrefix() {
    ObjectStorageLocation location = ObjectStorageLocation.parse("s3://my-bucket/exports/datahub");
    assertEquals(location.provider(), ObjectStorageProvider.S3);
    assertEquals(location.bucket(), "my-bucket");
    assertEquals(location.keyPrefix(), "exports/datahub");
    assertEquals(location.localRoot(), null);
  }

  @Test
  public void testParseS3BucketOnly() {
    ObjectStorageLocation location = ObjectStorageLocation.parse("s3://my-bucket");
    assertEquals(location.provider(), ObjectStorageProvider.S3);
    assertEquals(location.bucket(), "my-bucket");
    assertEquals(location.keyPrefix(), "");
    assertEquals(location.localRoot(), null);
  }

  @Test
  public void testParseS3BucketRootWithTrailingSlash() {
    ObjectStorageLocation location = ObjectStorageLocation.parse("s3://my-bucket/");
    assertEquals(location.provider(), ObjectStorageProvider.S3);
    assertEquals(location.bucket(), "my-bucket");
    assertEquals(location.keyPrefix(), "");
  }

  @Test
  public void testParseGcsBucketOnly() {
    ObjectStorageLocation location = ObjectStorageLocation.parse("gs://my-bucket");
    assertEquals(location.provider(), ObjectStorageProvider.GCS);
    assertEquals(location.bucket(), "my-bucket");
    assertEquals(location.keyPrefix(), "");
  }

  @Test
  public void testParseFileUri() {
    ObjectStorageLocation location =
        ObjectStorageLocation.parse("file:///tmp/datahub-object-storage");
    assertEquals(location.provider(), ObjectStorageProvider.LOCAL);
    assertEquals(location.localRoot(), "/tmp/datahub-object-storage");
    assertEquals(location.bucket(), null);
  }

  @Test
  public void testResolveExplicitUri() {
    ObjectStorageLocation location =
        ObjectStorageLocation.resolve("gs://bucket/prefix", "ignored", "ignored", "s3").get();
    assertEquals(location.provider(), ObjectStorageProvider.GCS);
    assertEquals(location.bucket(), "bucket");
    assertEquals(location.keyPrefix(), "prefix");
  }

  @Test
  public void testSynthesizeFromLegacyS3BucketOnly() {
    ObjectStorageLocation location =
        ObjectStorageLocation.resolve(null, "my-bucket", null, "s3").get();
    assertEquals(location.provider(), ObjectStorageProvider.S3);
    assertEquals(location.bucket(), "my-bucket");
    assertEquals(location.keyPrefix(), "");
  }

  @Test
  public void testSynthesizeFromLegacyGcsBucketPrefixOnly() {
    ObjectStorageLocation location =
        ObjectStorageLocation.resolve(null, "gs://my-bucket", null, null).get();
    assertEquals(location.provider(), ObjectStorageProvider.GCS);
    assertEquals(location.bucket(), "my-bucket");
    assertEquals(location.keyPrefix(), "");
  }

  @Test
  public void testSynthesizeFromLegacyS3() {
    ObjectStorageLocation location =
        ObjectStorageLocation.resolve(null, "my-bucket", "exports/datahub", "s3").get();
    assertEquals(location.provider(), ObjectStorageProvider.S3);
    assertEquals(location.bucket(), "my-bucket");
    assertEquals(location.keyPrefix(), "exports/datahub");
  }

  @Test
  public void testSynthesizeFromLegacyGcsBucketPrefix() {
    ObjectStorageLocation location =
        ObjectStorageLocation.resolve(null, "gs://my-bucket", "exports/datahub", null).get();
    assertEquals(location.provider(), ObjectStorageProvider.GCS);
    assertEquals(location.bucket(), "my-bucket");
    assertEquals(location.keyPrefix(), "exports/datahub");
  }

  @Test
  public void testSynthesizeFromLegacyLocal() {
    ObjectStorageLocation location =
        ObjectStorageLocation.resolve(null, null, "/tmp/datahub-object-storage", "local").get();
    assertEquals(location.provider(), ObjectStorageProvider.LOCAL);
    assertEquals(location.localRoot(), "/tmp/datahub-object-storage");
  }

  @Test
  public void testSynthesizeFromLegacyDefaultsToS3WhenBucketSet() {
    ObjectStorageLocation location =
        ObjectStorageLocation.resolve(null, "my-bucket", "prefix", null).get();
    assertEquals(location.provider(), ObjectStorageProvider.S3);
    assertEquals(location.bucket(), "my-bucket");
    assertEquals(location.keyPrefix(), "prefix");
  }

  @Test
  public void testResolveEmptyWhenUnconfigured() {
    assertFalse(ObjectStorageLocation.resolve(null, null, null, null).isPresent());
    assertFalse(ObjectStorageLocation.resolve("", "", "", "").isPresent());
    assertFalse(ObjectStorageLocation.resolve(null, "", "", "local").isPresent());
  }

  @Test
  public void testParseUnsupportedScheme() {
    assertThrows(
        IllegalArgumentException.class, () -> ObjectStorageLocation.parse("http://bucket"));
  }

  @Test
  public void testParseCloudUriRejectsEmptyBucket() {
    assertThrows(IllegalArgumentException.class, () -> ObjectStorageLocation.parse("s3:///prefix"));
  }

  @Test
  public void testParseFileUriRejectsMissingPath() {
    assertThrows(IllegalArgumentException.class, () -> ObjectStorageLocation.parse("file://"));
  }

  @Test
  public void testSynthesizeLocalRequiresAbsolutePath() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ObjectStorageLocation.resolve(null, null, "relative/path", "local"));
  }
}
