package com.linkedin.metadata.opensearch.embedded;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Prevents direct Java and Gradle launches from upgrading another major's local data. */
final class OpenSearchDataDirectory {
  private OpenSearchDataDirectory() {}

  static void prepare(Path directory, String major) throws IOException {
    Path resolved = directory.toFile().getCanonicalFile().toPath();
    // Also protect the reserved directory when callers bypass the Python manager.
    for (Path parent = resolved; parent != null; parent = parent.getParent()) {
      if (parent.endsWith(Path.of(".datahub", "local", "opensearch"))) {
        throw new IllegalArgumentException("Reserved OpenSearch directory: " + parent);
      }
    }
    String name = resolved.getFileName().toString();
    if (name.matches("opensearch-[23]") && !name.equals("opensearch-" + major)) {
      throw new IllegalArgumentException("OpenSearch " + major + " cannot use " + directory);
    }
    Path marker = resolved.resolve("opensearch-major");
    // Data without a marker belongs to the original 3.x-only launcher.
    if (!Files.exists(marker) && Files.exists(resolved.resolve("data")) && !major.equals("3")) {
      throw new IllegalArgumentException("Unmarked OpenSearch 3 data at " + directory);
    }
    Files.createDirectories(resolved);
    try {
      Files.writeString(marker, major + "\n", StandardOpenOption.CREATE_NEW);
    } catch (FileAlreadyExistsException existing) {
      if (!Files.readString(marker).trim().equals(major)) {
        throw new IllegalArgumentException("OpenSearch data major mismatch at " + directory);
      }
    }
  }
}
