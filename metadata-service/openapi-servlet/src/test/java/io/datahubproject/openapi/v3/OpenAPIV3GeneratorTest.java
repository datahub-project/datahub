package io.datahubproject.openapi.v3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import io.swagger.v3.core.util.Yaml;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.media.Schema;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
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
    ConfigurationProvider configurationProvider = new ConfigurationProvider();
    configurationProvider.setFeatureFlags(new FeatureFlags());

    OpenAPI openAPI = OpenAPIV3Generator.generateOpenApiSpec(er, configurationProvider);
    String openapiYaml = Yaml.pretty(openAPI);
    Files.write(
        Path.of(getClass().getResource("/").getPath(), "open-api.yaml"),
        openapiYaml.getBytes(StandardCharsets.UTF_8));

    assertTrue(openAPI.getComponents().getSchemas().size() > 900);
    assertTrue(openAPI.getComponents().getParameters().size() > 50);
    assertTrue(openAPI.getPaths().size() > 500);

    Schema datasetPropertiesSchema = openAPI.getComponents().getSchemas().get("DatasetProperties");
    List<String> requiredNames = datasetPropertiesSchema.getRequired();
    Map<String, Schema> properties = datasetPropertiesSchema.getProperties();

    // Assert required properties are non-nullable
    Schema customProperties = properties.get("customProperties");
    assertTrue(requiredNames.contains("customProperties"));
    assertFalse(customProperties.getNullable());

    // Assert non-required properties are nullable
    Schema name = properties.get("name");
    assertFalse(requiredNames.contains("name"));
    assertTrue(name.getNullable());

    // Assert non-required $ref properties are replaced by nullable { anyOf: [ $ref ] } objects
    Schema created = properties.get("created");
    assertFalse(requiredNames.contains("created"));
    assertEquals("object", created.getType());
    assertNull(created.get$ref());
    assertEquals(List.of(new Schema().$ref("#/components/schemas/TimeStamp")), created.getAnyOf());
    assertTrue(created.getNullable());

    // Assert systemMetadata property on response schema is optional.
    Map<String, Schema> datasetPropertiesResponseSchemaProps =
        openAPI
            .getComponents()
            .getSchemas()
            .get("DatasetPropertiesAspectResponse_v3")
            .getProperties();
    Schema systemMetadata = datasetPropertiesResponseSchemaProps.get("systemMetadata");
    assertEquals("object", systemMetadata.getType());
    assertNull(systemMetadata.get$ref());
    assertEquals(
        List.of(new Schema().$ref("#/components/schemas/SystemMetadata")),
        systemMetadata.getAnyOf());
    assertTrue(systemMetadata.getNullable());

    // Assert enum property is string.
    Schema fabricType = openAPI.getComponents().getSchemas().get("FabricType");
    assertEquals("string", fabricType.getType());
    assertFalse(fabricType.getEnum().isEmpty());

    Map<String, Schema> batchProperties =
        openAPI
            .getComponents()
            .getSchemas()
            .get("BatchGetContainerEntityRequest_v3")
            .getProperties();
    batchProperties.entrySet().stream()
        .filter(entry -> !entry.getKey().equals("urn"))
        .forEach(
            entry ->
                assertEquals(
                    "#/components/schemas/BatchGetRequestBody", entry.getValue().get$ref()));
  }
}
