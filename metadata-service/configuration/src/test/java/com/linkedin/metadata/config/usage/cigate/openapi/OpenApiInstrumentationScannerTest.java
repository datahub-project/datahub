package com.linkedin.metadata.config.usage.cigate.openapi;

import com.linkedin.metadata.config.usage.cigate.HandlerInstrumentationSurface;
import java.nio.file.Files;
import java.nio.file.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

public class OpenApiInstrumentationScannerTest {

  @Test
  public void testDetectsMissingInstrumentation() throws Exception {
    Path tempDir = Files.createTempDirectory("openapi-scan-test");
    Path sourceRoot = tempDir.resolve("metadata-service/openapi-servlet/src/main/java/demo");
    Files.createDirectories(sourceRoot);
    Files.writeString(
        sourceRoot.resolve("DemoController.java"),
        """
        class DemoController {
          void handler() {
            OperationContext.asSession(
                systemOperationContext,
                RequestContext.builder()
                    .buildOpenapi("urn:li:corpuser:foo", request, "demoAction", List.of()),
                authorizer,
                authentication,
                true);
          }
        }
        """);

    HandlerInstrumentationSurface surface = OpenApiInstrumentationScanner.scan(tempDir);
    Assert.assertEquals(surface.handlers().size(), 1);
    Assert.assertFalse(surface.handlers().get(0).instrumented());
  }

  @Test
  public void testDetectsWithUsageOperation() throws Exception {
    Path tempDir = Files.createTempDirectory("openapi-scan-test");
    Path sourceRoot = tempDir.resolve("metadata-service/openapi-servlet/src/main/java/demo");
    Files.createDirectories(sourceRoot);
    Files.writeString(
        sourceRoot.resolve("DemoController.java"),
        """
        class DemoController {
          void handler() {
            OperationContext.asSession(
                systemOperationContext,
                RequestContext.builder()
                    .buildOpenapi("urn:li:corpuser:foo", request, "demoAction", List.of())
                    .withUsageOperation(UsageOperation.METADATA_READ),
                authorizer,
                authentication,
                true);
          }
        }
        """);

    HandlerInstrumentationSurface surface = OpenApiInstrumentationScanner.scan(tempDir);
    Assert.assertEquals(surface.handlers().size(), 1);
    Assert.assertTrue(surface.handlers().get(0).instrumented());
    Assert.assertEquals(surface.handlers().get(0).usageOperation(), "metadata_read");
  }
}
