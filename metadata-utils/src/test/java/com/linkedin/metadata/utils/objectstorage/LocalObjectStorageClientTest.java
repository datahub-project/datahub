package com.linkedin.metadata.utils.objectstorage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.testng.annotations.Test;

public class LocalObjectStorageClientTest {

  @Test
  public void testPutObjectCreatesFile() throws Exception {
    Path root = Files.createTempDirectory("object-storage-local");
    LocalObjectStorageClient client = new LocalObjectStorageClient(root.toString());

    client.putObject("exports/2026/report.bin", "payload".getBytes());

    Path written = root.resolve("exports/2026/report.bin");
    assertTrue(Files.exists(written));
    assertEquals(Files.readString(written), "payload");
  }

  @Test
  public void testPutObjectOverwrites() throws Exception {
    Path root = Files.createTempDirectory("object-storage-local");
    LocalObjectStorageClient client = new LocalObjectStorageClient(root.toString());

    client.putObject("exports/out.bin", "first".getBytes());
    client.putObject("exports/out.bin", "second".getBytes());

    assertEquals(Files.readString(root.resolve("exports/out.bin")), "second");
  }

  @Test
  public void testPutObjectAcceptsLeadingSlash() throws Exception {
    Path root = Files.createTempDirectory("object-storage-local");
    LocalObjectStorageClient client = new LocalObjectStorageClient(root.toString());

    client.putObject("/exports/report.bin", "payload".getBytes());

    Path written = root.resolve("exports/report.bin");
    assertTrue(Files.exists(written));
    assertEquals(Files.readString(written), "payload");
  }

  @Test
  public void testPutObjectRejectsTraversal() throws Exception {
    Path root = Files.createTempDirectory("object-storage-local");
    LocalObjectStorageClient client = new LocalObjectStorageClient(root.toString());

    assertThrows(
        ObjectStoragePathException.class,
        () -> client.putObject("exports/../outside.bin", "x".getBytes()));
  }

  @Test
  public void testIsConfigured() throws Exception {
    assertFalse(new LocalObjectStorageClient(null).isConfigured());
    assertFalse(new LocalObjectStorageClient("").isConfigured());
    Path root = Files.createTempDirectory("object-storage-configured");
    assertTrue(new LocalObjectStorageClient(root.toString()).isConfigured());
  }

  @Test
  public void testConstructorCreatesMissingRoot() throws Exception {
    Path base = Files.createTempDirectory("object-storage-base");
    Path root = base.resolve("missing").resolve("nested").resolve("root");
    assertFalse(Files.exists(root));

    LocalObjectStorageClient client = new LocalObjectStorageClient(root.toString());

    assertTrue(Files.isDirectory(root));
    assertTrue(client.isConfigured());
  }

  @Test
  public void testConstructorToleratesExistingRoot() throws Exception {
    Path root = Files.createTempDirectory("object-storage-existing");
    assertTrue(Files.isDirectory(root));

    LocalObjectStorageClient client = new LocalObjectStorageClient(root.toString());

    assertTrue(Files.isDirectory(root));
    assertTrue(client.isConfigured());
  }

  @Test
  public void testConstructorToleratesPartialExistingPath() throws Exception {
    Path base = Files.createTempDirectory("object-storage-partial");
    Path existingParent = base.resolve("already-there");
    Files.createDirectories(existingParent);
    Path root = existingParent.resolve("child").resolve("root");
    assertTrue(Files.isDirectory(existingParent));
    assertFalse(Files.exists(root));

    LocalObjectStorageClient client = new LocalObjectStorageClient(root.toString());

    assertTrue(Files.isDirectory(root));
    assertTrue(client.isConfigured());
  }

  @Test
  public void testPutObjectRejectsUnconfiguredRoot() {
    LocalObjectStorageClient client = new LocalObjectStorageClient(null);
    assertThrows(
        IllegalStateException.class, () -> client.putObject("exports/out.bin", new byte[] {1}));
  }
}
