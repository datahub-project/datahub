package com.linkedin.metadata.usage;

import static org.testng.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.testng.annotations.Test;

/**
 * Ensures the legacy product-usage rollup package and API usage aggregation remain separate.
 * Package directory names under {@code metadata/billing} are commercial overlays.
 */
public class UsageRollupSystemsBoundaryTest {

  private static final Path METADATA_IO_SRC = Path.of("src/main/java/com/linkedin/metadata");

  @Test
  public void ossUsagePackagesDoNotImportProductUsageRollup() throws IOException {
    Path usageRoot = METADATA_IO_SRC.resolve("usage");
    try (Stream<Path> paths = Files.walk(usageRoot)) {
      paths
          .filter(path -> path.toString().endsWith(".java"))
          // Commercial overlay under usage/billing may integrate with product rollup packages.
          .filter(path -> !path.toString().contains("/usage/billing/"))
          .forEach(
              path -> {
                try {
                  for (String line : Files.readAllLines(path)) {
                    if (line.startsWith("import ")
                        && line.contains("com.linkedin.metadata.billing.rollup.")) {
                      fail(
                          "OSS usage package must not import product-usage rollup: "
                              + path
                              + " -> "
                              + line);
                    }
                  }
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
    }
  }

  @Test
  public void productUsageRollupDoesNotImportUsageStore() throws IOException {
    Path rollupRoot = METADATA_IO_SRC.resolve("billing/rollup");
    Path rollupStore = rollupRoot.resolve("InMemoryUsageRollupStore.java");
    if (!Files.exists(rollupStore)) {
      return; // Deployments without the product-usage rollup package skip this check.
    }
    List<String> lines = Files.readAllLines(rollupStore);
    for (String line : lines) {
      if (line.startsWith("import ") && line.contains("com.linkedin.metadata.usage.store.")) {
        fail("product-usage rollup must not import usage.store: " + line);
      }
    }
    try (Stream<Path> paths = Files.walk(rollupRoot)) {
      paths
          .filter(path -> path.toString().endsWith(".java"))
          .forEach(
              path -> {
                try {
                  for (String line : Files.readAllLines(path)) {
                    if (line.startsWith("import ")
                        && line.contains("com.linkedin.metadata.usage.")) {
                      fail("product-usage rollup must not import usage.*: " + path + " -> " + line);
                    }
                  }
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
    }
  }
}
