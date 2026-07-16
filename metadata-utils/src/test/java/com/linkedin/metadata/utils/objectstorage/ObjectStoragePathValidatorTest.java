package com.linkedin.metadata.utils.objectstorage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.testng.annotations.Test;

public class ObjectStoragePathValidatorTest {

  @Test
  public void testResolveUnderRootHappyPath() throws Exception {
    Path base = Files.createTempDirectory("object-storage-base");
    Path resolved = ObjectStoragePathValidator.resolveUnderRoot(base, "exports/2026/report.bin");
    assertTrue(resolved.startsWith(base));
    assertEquals(resolved.getFileName().toString(), "report.bin");
  }

  @Test
  public void testRejectParentTraversal() throws Exception {
    Path base = Files.createTempDirectory("object-storage-base");
    assertThrows(
        ObjectStoragePathException.class,
        () -> ObjectStoragePathValidator.resolveUnderRoot(base, "exports/../etc/passwd"));
  }

  @Test
  public void testRejectAbsolutePath() throws Exception {
    Path base = Files.createTempDirectory("object-storage-base");
    assertThrows(
        ObjectStoragePathException.class,
        () -> ObjectStoragePathValidator.resolveUnderRoot(base, "/etc/passwd"));
  }

  @Test
  public void testRejectLeadingParent() throws Exception {
    Path base = Files.createTempDirectory("object-storage-base");
    assertThrows(
        ObjectStoragePathException.class,
        () -> ObjectStoragePathValidator.resolveUnderRoot(base, "../outside"));
  }

  @Test
  public void testRejectNestedTraversal() throws Exception {
    Path base = Files.createTempDirectory("object-storage-base");
    assertThrows(
        ObjectStoragePathException.class,
        () -> ObjectStoragePathValidator.resolveUnderRoot(base, "foo/../../secret"));
  }

  @Test
  public void testDecodeUrlEncodedPath() throws Exception {
    Path base = Files.createTempDirectory("object-storage-base");
    Path resolved =
        ObjectStoragePathValidator.resolveUnderRoot(base, "exports%2F2026%2Freport.bin");
    assertEquals(resolved.getFileName().toString(), "report.bin");
  }
}
