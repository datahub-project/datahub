package io.datahubproject.openapi.v3;

import static org.testng.Assert.assertTrue;

import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import io.swagger.v3.core.util.Yaml;
import io.swagger.v3.oas.models.OpenAPI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class OpenAPIV3GeneratorTest {
  public static final String BASE_DIRECTORY =
      "../../entity-registry/custom-test-model/build/plugins/models";

  @BeforeTest
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @Test
  public void testOpenApiSpecBuilder() throws Exception {
    ConfigEntityRegistry er =
        new ConfigEntityRegistry(
            OpenAPIV3GeneratorTest.class
                .getClassLoader()
                .getResourceAsStream("entity-registry.yml"));

    OpenAPI openAPI = OpenAPIV3Generator.generateOpenApiSpec(er);
    String openapiYaml = Yaml.pretty(openAPI);
    Files.write(
        Path.of(getClass().getResource("/").getPath(), "open-api.yaml"),
        openapiYaml.getBytes(StandardCharsets.UTF_8));

    assertTrue(openAPI.getComponents().getSchemas().size() > 900);
    assertTrue(openAPI.getComponents().getParameters().size() > 50);
    assertTrue(openAPI.getPaths().size() > 500);
  }
}
