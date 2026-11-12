package com.linkedin.gms.factory.systemmetadata;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.testng.annotations.Test;

/** Guards OSS entity count metrics factory boundaries for downstream extension overlays. */
public class EntityCountMetricsFactoryPlacementTest {

  private static final Path OSS_FACTORY =
      Path.of(
          "src/main/java/com/linkedin/gms/factory/systemmetadata/EntityCountMetricsFactory.java");

  @Test
  public void entityCountMetricsFactoryHasNoExtensionPackageImports() throws IOException {
    for (String line : Files.readAllLines(OSS_FACTORY)) {
      if (line.startsWith("import ")
          && (line.contains(".extensions.") || line.contains(".overlay."))) {
        fail("OSS factory must not import downstream extension packages: " + line);
      }
    }
  }

  @Test
  public void entityCountMetricsFactoryRegistersSingleSinkComposerBean() throws IOException {
    List<String> lines = Files.readAllLines(OSS_FACTORY);
    long composerBeanMethods =
        lines.stream()
            .filter(line -> line.strip().startsWith("public EntityCountMetricsSinkComposer"))
            .count();
    assertEquals(
        composerBeanMethods,
        1,
        "OSS factory must register exactly one EntityCountMetricsSinkComposer bean");
  }
}
