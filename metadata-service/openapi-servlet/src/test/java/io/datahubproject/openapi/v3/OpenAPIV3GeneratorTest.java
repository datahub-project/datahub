package io.datahubproject.openapi.v3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import io.swagger.parser.OpenAPIParser;
import io.swagger.v3.core.util.Json;
import io.swagger.v3.core.util.Yaml;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.parser.core.models.ParseOptions;
import io.swagger.v3.parser.core.models.SwaggerParseResult;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class OpenAPIV3GeneratorTest {
  public static final String BASE_DIRECTORY =
      "../../entity-registry/custom-test-model/build/plugins/models";

  private OpenAPI openAPI;

  @BeforeTest
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);

    ConfigEntityRegistry er =
        new ConfigEntityRegistry(
            OpenAPIV3GeneratorTest.class
                .getClassLoader()
                .getResourceAsStream("entity-registry.yml"));
    ConfigurationProvider configurationProvider = new ConfigurationProvider();
    configurationProvider.setFeatureFlags(new FeatureFlags());

    openAPI = OpenAPIV3Generator.generateOpenApiSpec(er, configurationProvider);
  }

  @Test
  void testSchemaValidation() {
    // Convert the OpenAPI object to JSON
    String openApiJson = Json.pretty(openAPI);

    // Parse and validate the OpenAPI spec
    ParseOptions options = new ParseOptions();
    options.setResolve(true);
    options.setResolveFully(true);
    options.setValidateInternalRefs(true); // Important for schema validation

    SwaggerParseResult result = new OpenAPIParser().readContents(openApiJson, null, options);

    // Check for schema validation errors
    if (result.getMessages() != null && !result.getMessages().isEmpty()) {
      System.out.println("Schema validation errors:");
      for (String message : result.getMessages()) {
        System.out.println(" - " + message);
      }
    }

    assertTrue(
        result.getMessages() == null || result.getMessages().isEmpty(),
        "Valid OpenAPI schema should not have validation errors");

    assertNotNull(result.getOpenAPI(), "Parsed OpenAPI should not be null");
  }

  @Test
  public void testOpenAPICounts() throws IOException {
    String openapiYaml = Yaml.pretty(openAPI);
    Files.write(
        Path.of(getClass().getResource("/").getPath(), "open-api.yaml"),
        openapiYaml.getBytes(StandardCharsets.UTF_8));

    assertTrue(openAPI.getComponents().getSchemas().size() > 1000);
    assertTrue(openAPI.getComponents().getParameters().size() > 60);
    assertTrue(openAPI.getPaths().size() > 650);
  }

  @Test
  public void testEnumString() {
    // Assert enum property is string.
    Schema fabricType = openAPI.getComponents().getSchemas().get("FabricType");
    assertEquals("string", fabricType.getType());
    assertFalse(fabricType.getEnum().isEmpty());
  }

  @Test
  public void testBatchProperties() {
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

  @Test
  public void testDatasetProperties() {
    Schema datasetPropertiesSchema = openAPI.getComponents().getSchemas().get("DatasetProperties");
    List<String> requiredNames = datasetPropertiesSchema.getRequired();
    Map<String, Schema> properties = datasetPropertiesSchema.getProperties();

    // Assert required properties are non-nullable
    Schema customProperties = properties.get("customProperties");
    assertFalse(requiredNames.contains("customProperties")); // not required due to default
    assertFalse(
        customProperties
            .getNullable()); // it is however still not optional, therefore null is not allowed

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
  }

  @Test
  public void testNotebookInfo() {
    Schema notebookInfoSchema = openAPI.getComponents().getSchemas().get("NotebookInfo");
    Set<String> requiredNames = new HashSet(notebookInfoSchema.getRequired());
    Map<String, Schema> properties = notebookInfoSchema.getProperties();

    assertEquals(requiredNames, Set.of("title", "changeAuditStamps")); // required without optional

    for (String reqProp : requiredNames) {
      Schema customProperties = properties.get(reqProp);
      assertFalse(customProperties.getNullable()); // null is not allowed
    }

    Schema changeAuditStamps = properties.get("changeAuditStamps");
    assertEquals(changeAuditStamps.get$ref(), "#/components/schemas/ChangeAuditStamps");
  }

  @Test
  public void testChangeAuditStamps() {
    Schema schema = openAPI.getComponents().getSchemas().get("ChangeAuditStamps");
    List<String> requiredNames = schema.getRequired();
    assertEquals(requiredNames, List.of());

    Map<String, Schema> properties = schema.getProperties();
    assertEquals(properties.keySet(), Set.of("created", "lastModified", "deleted"));

    Set.of("created", "lastModified")
        .forEach(
            prop -> {
              assertFalse(properties.get(prop).getNullable());
            });
    Set.of("deleted")
        .forEach(
            prop -> {
              assertTrue(properties.get(prop).getNullable());
            });
  }

  @Test
  public void testDatasetProfile() {
    Schema datasetProfileSchema = openAPI.getComponents().getSchemas().get("DatasetProfile");
    List<String> requiredNames = datasetProfileSchema.getRequired();
    assertFalse(requiredNames.contains("partitionSpec"));
  }

  @Test
  public void testExcluded() {
    assertFalse(
        openAPI.getComponents().getSchemas().containsKey("DataHubOpenAPISchemaEntityRequest_v3"));
    assertFalse(
        openAPI.getComponents().getSchemas().containsKey("DataHubOpenAPISchemaEntityResponse_v3"));
    assertFalse(openAPI.getComponents().getSchemas().containsKey("DataHubOpenAPISchemaKey"));
    assertFalse(
        openAPI
            .getComponents()
            .getSchemas()
            .containsKey("DataHubOpenAPISchemaKeyAspectRequest_v3"));
    assertFalse(
        openAPI
            .getComponents()
            .getSchemas()
            .containsKey("DataHubOpenAPISchemaKeyAspectResponse_v3"));
  }
}
