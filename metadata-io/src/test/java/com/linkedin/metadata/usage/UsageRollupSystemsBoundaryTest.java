package com.linkedin.metadata.usage;

import static org.testng.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.testng.annotations.Test;

/** Ensures billing rollup and API usage aggregation remain separate packages. */
public class UsageRollupSystemsBoundaryTest {

  private static final Path METADATA_IO_SRC = Path.of("src/main/java/com/linkedin/metadata");

  @Test
  public void usagePackagesDoNotImportBillingRollup() throws IOException {
    Path usageRoot = METADATA_IO_SRC.resolve("usage");
    try (Stream<Path> paths = Files.walk(usageRoot)) {
      paths
          .filter(path -> path.toString().endsWith(".java"))
          .forEach(
              path -> {
                try {
                  for (String line : Files.readAllLines(path)) {
                    if (line.startsWith("import ")
                        && line.contains("com.linkedin.metadata.billing.rollup.")) {
                      fail("usage package must not import billing.rollup: " + path + " -> " + line);
                    }
                  }
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
    }
  }

  @Test
  public void billingRollupDoesNotImportUsageStore() throws IOException {
    Path rollupRoot = METADATA_IO_SRC.resolve("billing/rollup");
    Path rollupStore = rollupRoot.resolve("InMemoryUsageRollupStore.java");
    if (!Files.exists(rollupStore)) {
      return; // OSS deployments without billing rollup skip this boundary check.
    }
    List<String> lines = Files.readAllLines(rollupStore);
    for (String line : lines) {
      if (line.startsWith("import ") && line.contains("com.linkedin.metadata.usage.store.")) {
        fail("billing.rollup must not import usage.store: " + line);
      }
    }
    // Javadoc @see links are allowed; verify no compile-time imports across all rollup sources.
    try (Stream<Path> paths = Files.walk(rollupRoot)) {
      paths
          .filter(path -> path.toString().endsWith(".java"))
          .forEach(
              path -> {
                try {
                  for (String line : Files.readAllLines(path)) {
                    if (line.startsWith("import ")
                        && line.contains("com.linkedin.metadata.usage.")) {
                      fail("billing.rollup must not import usage.*: " + path + " -> " + line);
                    }
                  }
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
    }
  }
}
