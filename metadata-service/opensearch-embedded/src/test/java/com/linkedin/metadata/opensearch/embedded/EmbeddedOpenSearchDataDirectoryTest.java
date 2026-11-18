package com.linkedin.metadata.opensearch.embedded;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class EmbeddedOpenSearchDataDirectoryTest {
  @Rule public TemporaryFolder temporary = new TemporaryFolder();

  @Test
  public void markedDataCannotBeOpenedByAnotherMajor() throws Exception {
    Path directory = temporary.newFolder().toPath();
    OpenSearchDataDirectory.prepare(directory, "2");
    OpenSearchDataDirectory.prepare(directory, "2");
    assertThrows(
        IllegalArgumentException.class, () -> OpenSearchDataDirectory.prepare(directory, "3"));
    assertEquals("2", Files.readString(directory.resolve("opensearch-major")).trim());
  }

  @Test
  public void legacyDataIsOnlyAdoptedByThree() throws Exception {
    Path directory = temporary.newFolder().toPath();
    Files.createDirectory(directory.resolve("data"));
    assertThrows(
        IllegalArgumentException.class, () -> OpenSearchDataDirectory.prepare(directory, "2"));
    OpenSearchDataDirectory.prepare(directory, "3");
    assertEquals("3", Files.readString(directory.resolve("opensearch-major")).trim());
  }

  @Test
  public void reservedAndOtherMajorDirectoriesAreUntouched() throws Exception {
    Path root = temporary.newFolder().toPath();
    for (Path directory :
        new Path[] {root.resolve(".datahub/local/opensearch"), root.resolve("opensearch-3")}) {
      assertThrows(
          IllegalArgumentException.class, () -> OpenSearchDataDirectory.prepare(directory, "2"));
      assertFalse(Files.exists(directory));
    }
  }
}
