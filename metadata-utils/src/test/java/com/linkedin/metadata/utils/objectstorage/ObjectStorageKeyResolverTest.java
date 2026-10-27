package com.linkedin.metadata.utils.objectstorage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import org.testng.annotations.Test;

public class ObjectStorageKeyResolverTest {

  @Test
  public void testJoinKeyEmptyPath() {
    assertEquals(
        ObjectStorageKeyResolver.joinKey("", "exports/run.bin", ObjectStorageProvider.S3),
        "exports/run.bin");
  }

  @Test
  public void testJoinKeyWithPrefix() {
    assertEquals(
        ObjectStorageKeyResolver.joinKey(
            "infra-prefix", "exports/run.bin", ObjectStorageProvider.S3),
        "infra-prefix/exports/run.bin");
  }

  @Test
  public void testJoinKeyTrimsSlashes() {
    assertEquals(
        ObjectStorageKeyResolver.joinKey(
            "/infra-prefix/", "/exports/run.bin", ObjectStorageProvider.GCS),
        "infra-prefix/exports/run.bin");
  }

  @Test
  public void testJoinKeyLocalRejectsTraversal() {
    assertThrows(
        ObjectStoragePathException.class,
        () ->
            ObjectStorageKeyResolver.joinKey(
                "/tmp/root", "exports/../etc/passwd", ObjectStorageProvider.LOCAL));
  }
}
