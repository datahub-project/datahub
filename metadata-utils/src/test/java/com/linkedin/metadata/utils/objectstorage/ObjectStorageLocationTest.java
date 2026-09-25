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

  // ---------------------------------------------------------------------------
  // parseDocument — a URI that names one object, split into root + key
  // ---------------------------------------------------------------------------

  @Test
  public void testParseDocumentCloudSplitsTrailingSegmentOffThePrefix() {
    ObjectStorageLocation.Document document =
        ObjectStorageLocation.parseDocument("s3://my-bucket/exports/matrix.json");
    assertEquals(document.root().provider(), ObjectStorageProvider.S3);
    assertEquals(document.root().bucket(), "my-bucket");
    assertEquals(document.root().keyPrefix(), "exports");
    assertEquals(document.objectKey(), "matrix.json");
  }

  @Test
  public void testParseDocumentCloudAtBucketRootHasNoPrefix() {
    ObjectStorageLocation.Document document =
        ObjectStorageLocation.parseDocument("gs://my-bucket/matrix.json");
    assertEquals(document.root().provider(), ObjectStorageProvider.GCS);
    assertEquals(document.root().bucket(), "my-bucket");
    assertEquals(document.root().keyPrefix(), "");
    assertEquals(document.objectKey(), "matrix.json");
  }

  @Test
  public void testParseDocumentLocalRootsAtTheParentDirectory() {
    // The client is rooted at a directory, so the file name has to become the key.
    ObjectStorageLocation.Document document =
        ObjectStorageLocation.parseDocument("file:///var/lib/datahub/matrix.json");
    assertEquals(document.root().provider(), ObjectStorageProvider.LOCAL);
    assertEquals(document.root().localRoot(), "/var/lib/datahub");
    assertEquals(document.objectKey(), "matrix.json");
  }

  @Test
  public void testParseDocumentRejectsUriThatNamesNoObject() {
    // A bucket root or a filesystem root is not a readable document.
    assertThrows(
        IllegalArgumentException.class,
        () -> ObjectStorageLocation.parseDocument("s3://my-bucket"));
    assertThrows(
        IllegalArgumentException.class, () -> ObjectStorageLocation.parseDocument("file:///"));
  }

  @Test
  public void testParseDocumentRejectsUnsupportedScheme() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ObjectStorageLocation.parseDocument("ftp://host/matrix.json"));
  }

  // ---------------------------------------------------------------------------
  // Scheme case — case-insensitive per RFC 3986 §3.1
  // ---------------------------------------------------------------------------

  @Test
  public void testParseAcceptsAnySchemeCase() {
    for (String uri : new String[] {"S3://my-bucket/exports", "s3://my-bucket/exports"}) {
      ObjectStorageLocation location = ObjectStorageLocation.parse(uri);
      assertEquals(location.provider(), ObjectStorageProvider.S3, uri);
      assertEquals(location.bucket(), "my-bucket", uri);
      assertEquals(location.keyPrefix(), "exports", uri);
    }
    assertEquals(
        ObjectStorageLocation.parse("GS://my-bucket/exports").provider(),
        ObjectStorageProvider.GCS);
    assertEquals(
        ObjectStorageLocation.parse("FILE:///var/lib/datahub").provider(),
        ObjectStorageProvider.LOCAL);
    assertEquals(
        ObjectStorageLocation.parse("FiLe:///var/lib/datahub").localRoot(), "/var/lib/datahub");
  }

  @Test
  public void testParseFoldsOnlyTheSchemeNotTheKey() {
    // Bucket names, S3 keys and GCS object names are case-sensitive, so nothing past the scheme may
    // be lowercased along with it.
    ObjectStorageLocation location =
        ObjectStorageLocation.parse("S3://My-Bucket/Exports/CamelCase");
    assertEquals(location.bucket(), "My-Bucket");
    assertEquals(location.keyPrefix(), "Exports/CamelCase");
  }

  @Test
  public void testParseDocumentAcceptsAnySchemeCase() {
    ObjectStorageLocation.Document document =
        ObjectStorageLocation.parseDocument("S3://My-Bucket/Dir/Matrix.json");
    assertEquals(document.root().provider(), ObjectStorageProvider.S3);
    assertEquals(document.root().bucket(), "My-Bucket");
    assertEquals(document.root().keyPrefix(), "Dir");
    assertEquals(document.objectKey(), "Matrix.json");
  }

  @Test
  public void testResolveAcceptsAnySchemeCaseInLegacyBucket() {
    // The legacy bucket value may itself carry a scheme; it is matched the same way.
    ObjectStorageLocation location =
        ObjectStorageLocation.resolve(null, "S3://my-bucket", "exports", null).orElseThrow();
    assertEquals(location.provider(), ObjectStorageProvider.S3);
    assertEquals(location.bucket(), "my-bucket");
    assertEquals(location.keyPrefix(), "exports");
  }
}
