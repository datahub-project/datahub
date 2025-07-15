package io.datahubproject.openapi.v3;

import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import io.swagger.parser.OpenAPIParser;
import io.swagger.v3.core.util.Json;
import io.swagger.v3.core.util.Yaml;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.parser.OpenAPIV3Parser;
import io.swagger.v3.parser.core.models.ParseOptions;
import io.swagger.v3.parser.core.models.SwaggerParseResult;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class OpenAPIV3GeneratorTest {
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
    configurationProvider.setElasticSearch(TEST_ES_SEARCH_CONFIG);
    configurationProvider.setSearchService(TEST_SEARCH_SERVICE_CONFIG);

    openAPI = OpenAPIV3Generator.generateOpenApiSpec(er, configurationProvider);
  }

  @Test
  void testSchemaValidation() {
    // Convert the OpenAPI object to JSON
    String openApiJson = Json.pretty(openAPI);

    // Parse and validate the OpenAPI spec
    ParseOptions options = new ParseOptions();
    options.setResolve(true);
    options.setResolveFully(false); // requires a lot of memory
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
  void testIncrementalReferenceResolution() throws IOException {
    // This test demonstrates resolving references incrementally
    // useful for very large specs where you need to process piece by piece

    File tempFile = File.createTempFile("openapi", ".json");
    tempFile.deleteOnExit();

    try (FileWriter writer = new FileWriter(tempFile)) {
      Json.mapper().writeValue(writer, openAPI);
    }

    // Parse without resolution
    ParseOptions options = new ParseOptions();
    options.setResolve(false); // Don't resolve at all initially

    OpenAPIV3Parser parser = new OpenAPIV3Parser();
    OpenAPI parsedSpec = parser.read(tempFile.getAbsolutePath(), null, options);

    assertNotNull(parsedSpec, "Should be able to parse without resolution");

    // Count total references first
    AtomicInteger totalRefs = new AtomicInteger(0);
    countAllReferences(parsedSpec, totalRefs);

    System.out.println("Total references found: " + totalRefs.get());

    // Now resolve specific paths on demand
    AtomicInteger resolvedCount = new AtomicInteger(0);
    int maxResolutionsPerBatch = 10; // Process in small batches

    // Load the JSON for reference validation
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rootNode = mapper.readTree(tempFile);

    if (parsedSpec.getPaths() != null) {
      List<Map.Entry<String, PathItem>> pathEntries =
          new ArrayList<>(parsedSpec.getPaths().entrySet());

      for (int i = 0; i < pathEntries.size(); i += maxResolutionsPerBatch) {
        int end = Math.min(i + maxResolutionsPerBatch, pathEntries.size());
        List<Map.Entry<String, PathItem>> batch = pathEntries.subList(i, end);

        // Resolve just this batch
        resolveBatch(batch, rootNode, resolvedCount);

        // In a real scenario, you might process/validate the batch here
        // then allow it to be garbage collected before moving to the next batch
      }
    }

    // Also check components for references
    if (parsedSpec.getComponents() != null && parsedSpec.getComponents().getSchemas() != null) {
      parsedSpec
          .getComponents()
          .getSchemas()
          .forEach(
              (name, schema) -> {
                countSchemaReferences(schema, rootNode, resolvedCount);
              });
    }

    System.out.println(
        "Successfully resolved " + resolvedCount.get() + " references incrementally");

    // Only assert if there were references to resolve
    if (totalRefs.get() > 0) {
      assertTrue(resolvedCount.get() > 0, "Should have resolved at least some references");
    } else {
      System.out.println("No references found in the spec to resolve");
    }
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
    assertEquals(fabricType.getType(), "string");
    assertFalse(fabricType.getEnum().isEmpty());
  }

  @Test
  public void testBatchProperties() {
    Map<String, Schema> batchProps =
        openAPI
            .getComponents()
            .getSchemas()
            .get("BatchGetContainerEntityRequest_v3")
            .getProperties();

    batchProps.entrySet().stream()
        .filter(e -> !e.getKey().equals("urn"))
        .forEach(
            e -> {
              Schema<?> prop = e.getValue();

              // must be wrapped in oneOf
              assertNull(prop.get$ref());
              assertNotNull(prop.getOneOf());
              assertEquals(prop.getOneOf().size(), 2);

              boolean hasRef =
                  prop.getOneOf().stream()
                      .anyMatch(
                          s ->
                              "#/components/schemas/BatchGetRequestBody"
                                  .equals(((Schema<?>) s).get$ref()));
              boolean hasNull =
                  prop.getOneOf().stream().anyMatch(s -> "null".equals(((Schema<?>) s).getType()));

              assertTrue(hasRef, "oneOf must contain BatchGetRequestBody ref");
              assertTrue(hasNull, "oneOf must contain null type");
            });
  }

  @Test
  public void testDatasetProperties() {
    Schema datasetPropertiesSchema = openAPI.getComponents().getSchemas().get("DatasetProperties");
    Map<String, Schema> properties = datasetPropertiesSchema.getProperties();

    // Assert required properties are non-nullable
    Schema customProperties = properties.get("customProperties");
    assertNull(datasetPropertiesSchema.getRequired()); // not required due to defaults
    assertNull(customProperties.getNullable());
    assertEquals(customProperties.getType(), "object");
    assertEquals(
        customProperties.getTypes(),
        Set.of("object")); // it is however still not optional, therefore null is not allowed

    // Assert non-required properties are nullable
    Schema name = properties.get("name");
    assertNull(name.getNullable());
    assertEquals(name.getType(), "string");
    assertEquals(name.getTypes(), Set.of("string", "null"));

    // Assert non-required $ref properties are replaced by nullable { anyOf: [ $ref ] } objects
    Schema created = properties.get("created");
    assertNull(created.getNullable());
    assertNull(created.getType());
    assertNull(created.getTypes());
    assertNull(created.get$ref());
    assertEquals(created.getOneOf().size(), 2);

    assertTrue(
        created.getOneOf().stream()
            .anyMatch(s -> "#/components/schemas/TimeStamp".equals(((Schema<?>) s).get$ref())));
    assertTrue(
        created.getOneOf().stream()
            .anyMatch(
                s ->
                    ((Schema<?>) s).get$ref() == null && "null".equals(((Schema<?>) s).getType())));

    // Assert systemMetadata property on response schema is optional per v3.1.0
    Map<String, Schema> datasetPropertiesResponseSchemaProps =
        openAPI
            .getComponents()
            .getSchemas()
            .get("DatasetPropertiesAspectResponse_v3")
            .getProperties();
    Schema systemMetadataProperty = datasetPropertiesResponseSchemaProps.get("systemMetadata");
    assertNotNull(systemMetadataProperty);

    assertNull(systemMetadataProperty.get$ref());
    assertEquals(systemMetadataProperty.getTypes(), Set.of("object", "null"));
    assertNotNull(systemMetadataProperty.getOneOf());
    assertEquals(systemMetadataProperty.getOneOf().size(), 2);

    boolean hasSysMetaRef =
        systemMetadataProperty.getOneOf().stream()
            .anyMatch(s -> "#/components/schemas/SystemMetadata".equals(((Schema<?>) s).get$ref()));
    boolean hasNullAlt =
        systemMetadataProperty.getOneOf().stream()
            .anyMatch(s -> "null".equals(((Schema<?>) s).getType()));

    assertTrue(hasSysMetaRef, "systemMetadata oneOf must contain SystemMetadata ref");
    assertTrue(hasNullAlt, "systemMetadata oneOf must contain null type");
  }

  @Test
  public void testNotebookInfo() {
    Schema notebookInfoSchema = openAPI.getComponents().getSchemas().get("NotebookInfo");
    Set<String> requiredNames = new HashSet(notebookInfoSchema.getRequired());
    Map<String, Schema> properties = notebookInfoSchema.getProperties();

    assertEquals(requiredNames, Set.of("title", "changeAuditStamps")); // required without optional

    Schema titleSchema = properties.get("title");
    assertNull(titleSchema.getNullable());
    assertEquals(titleSchema.getTypes(), Set.of("string")); // null is not allowed
    assertEquals(titleSchema.getType(), "string");

    Schema changeAuditStampsSchema = properties.get("changeAuditStamps");
    assertNull(changeAuditStampsSchema.getNullable());
    assertNull(changeAuditStampsSchema.getTypes());
    assertNull(changeAuditStampsSchema.getType());
    assertNull(changeAuditStampsSchema.getOneOf()); // null is not allowed
    assertEquals(changeAuditStampsSchema.get$ref(), "#/components/schemas/ChangeAuditStamps");
  }

  @Test
  public void testChangeAuditStamps() {
    Schema schema = openAPI.getComponents().getSchemas().get("ChangeAuditStamps");
    List<String> requiredNames = schema.getRequired();
    assertNull(requiredNames);

    // Assert types
    assertEquals(schema.getTypes(), Set.of("object"));
    assertEquals(schema.getType(), "object");

    Map<String, Schema> properties = schema.getProperties();
    assertEquals(properties.keySet(), Set.of("created", "lastModified", "deleted"));

    Set.of("created", "lastModified")
        .forEach(
            prop -> {
              assertNull(properties.get(prop).getNullable());
              assertNull(properties.get(prop).getType());
              assertNull(properties.get(prop).getTypes());
              assertEquals(properties.get(prop).get$ref(), "#/components/schemas/AuditStamp");
            });
    Set.of("deleted")
        .forEach(
            prop -> {
              assertNull(properties.get(prop).getNullable());
              assertNull(properties.get(prop).getType());
              assertNull(properties.get(prop).getTypes());
              assertNull(properties.get(prop).get$ref());
              assertEquals(properties.get(prop).getOneOf().size(), 2);
              assertTrue(
                  properties.get(prop).getOneOf().stream()
                      .anyMatch(
                          s -> ((Schema) s).get$ref().equals("#/components/schemas/AuditStamp")));
              assertTrue(
                  properties.get(prop).getOneOf().stream()
                      .anyMatch(s -> ((Schema) s).get$ref() == null));
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

  @Test
  public void testNullableRefs() {
    Schema dataProcessInstanceProperties =
        openAPI.getComponents().getSchemas().get("DataProcessInstanceProperties");
    Map<String, Schema> properties = dataProcessInstanceProperties.getProperties();

    Schema optionalTypeSchema = properties.get("type");

    assertNull(optionalTypeSchema.getNullable());
    assertNull(optionalTypeSchema.getType());
    assertNull(optionalTypeSchema.getTypes());
    assertEquals(optionalTypeSchema.getOneOf().size(), 2);
    assertTrue(
        optionalTypeSchema.getOneOf().stream()
            .anyMatch(s -> ((Schema) s).get$ref().equals("#/components/schemas/DataProcessType")));
    assertTrue(
        optionalTypeSchema.getOneOf().stream().anyMatch(s -> ((Schema) s).get$ref() == null));
  }

  @Test
  public void testV31Enums() {
    Schema schema = openAPI.getComponents().getSchemas().get("VersioningScheme");
    assertEquals(schema.getType(), "string");

    assertTrue(
        Json.pretty(openAPI).contains("\"VersioningScheme\" : {\n        \"type\" : \"string\""));
  }

  @Test
  void testOpenAPISchemaCompliance() throws IOException {
    // This test validates the OpenAPI spec against the official OpenAPI JSON Schema

    // Convert OpenAPI to JSON
    ObjectMapper mapper = Json.mapper();
    JsonNode openApiNode = mapper.valueToTree(openAPI);

    // Determine OpenAPI version
    String openApiVersion = openApiNode.path("openapi").asText();
    assertNotNull(openApiVersion, "OpenAPI version must be specified");

    // Load the appropriate schema based on version
    JsonSchema schema;
    JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);

    try {
      if (openApiVersion.startsWith("3.1")) {
        schema = loadOpenAPI31Schema(schemaFactory);
      } else {
        fail("Unsupported OpenAPI version: " + openApiVersion);
        return;
      }
    } catch (Exception e) {
      // If we can't load the schema, skip the test with a warning
      System.err.println(
          "WARNING: Could not load OpenAPI schema for validation: " + e.getMessage());
      System.err.println(
          "Skipping schema compliance test. Consider adding the schema file to your test resources.");
      return;
    }

    // Validate the spec against the schema
    Set<ValidationMessage> validationMessages = schema.validate(openApiNode);

    // Log any validation errors
    if (!validationMessages.isEmpty()) {
      System.out.println("OpenAPI Schema Validation Errors:");
      validationMessages.forEach(
          msg -> {
            System.out.println(
                " - " + msg.getType() + ": " + msg.getMessage() + " at " + msg.getEvaluationPath());
          });
    }

    // Assert no validation errors
    assertTrue(
        validationMessages.isEmpty(),
        "OpenAPI spec should be compliant with OpenAPI "
            + openApiVersion
            + " schema. Found "
            + validationMessages.size()
            + " validation errors.");
  }

  @Test
  void testOpenAPISchemaComplianceWithDetails() throws IOException {
    // Enhanced version that provides more detailed validation information

    ObjectMapper mapper = Json.mapper();
    JsonNode openApiNode = mapper.valueToTree(openAPI);

    // Basic structure validation
    assertTrue(openApiNode.has("openapi"), "Must have 'openapi' field");
    assertTrue(openApiNode.has("info"), "Must have 'info' field");
    assertTrue(openApiNode.has("paths"), "Must have 'paths' field");

    // Validate info section
    JsonNode info = openApiNode.get("info");
    assertTrue(info.has("title"), "Info must have 'title'");
    assertTrue(info.has("version"), "Info must have 'version'");

    // Validate paths
    JsonNode paths = openApiNode.get("paths");
    assertTrue(paths.isObject(), "Paths must be an object");

    // Check each path
    Iterator<Map.Entry<String, JsonNode>> pathIterator = paths.fields();
    while (pathIterator.hasNext()) {
      Map.Entry<String, JsonNode> pathEntry = pathIterator.next();
      String path = pathEntry.getKey();
      JsonNode pathItem = pathEntry.getValue();

      // Validate path format
      assertTrue(path.startsWith("/"), "Path must start with '/': " + path);

      // Check for valid HTTP methods
      Set<String> validMethods =
          Set.of("get", "put", "post", "delete", "options", "head", "patch", "trace");
      Iterator<String> fieldNames = pathItem.fieldNames();
      while (fieldNames.hasNext()) {
        String field = fieldNames.next();
        if (!field.startsWith("$")
            && !field.equals("summary")
            && !field.equals("description")
            && !field.equals("servers")
            && !field.equals("parameters")) {
          assertTrue(
              validMethods.contains(field.toLowerCase()),
              "Invalid method '" + field + "' in path: " + path);
        }
      }
    }

    // If components exist, validate them
    if (openApiNode.has("components")) {
      JsonNode components = openApiNode.get("components");
      validateComponents(components);
    }
  }

  @Test
  void testOpenAPISpecificConstraints() {
    // Test specific OpenAPI constraints that might not be caught by JSON Schema

    // Check operation IDs are unique
    Set<String> operationIds = new HashSet<>();
    if (openAPI.getPaths() != null) {
      openAPI
          .getPaths()
          .forEach(
              (path, pathItem) -> {
                Stream.of(
                        pathItem.getGet(),
                        pathItem.getPost(),
                        pathItem.getPut(),
                        pathItem.getDelete(),
                        pathItem.getPatch(),
                        pathItem.getOptions(),
                        pathItem.getHead(),
                        pathItem.getTrace())
                    .filter(Objects::nonNull)
                    .forEach(
                        operation -> {
                          if (operation.getOperationId() != null) {
                            assertTrue(
                                operationIds.add(operation.getOperationId()),
                                "Duplicate operationId found: " + operation.getOperationId());
                          }
                        });
              });
    }

    // Check that all tags used in operations are defined
    Set<String> usedTags = new HashSet<>();
    if (openAPI.getPaths() != null) {
      openAPI
          .getPaths()
          .forEach(
              (path, pathItem) -> {
                Stream.of(
                        pathItem.getGet(),
                        pathItem.getPost(),
                        pathItem.getPut(),
                        pathItem.getDelete(),
                        pathItem.getPatch(),
                        pathItem.getOptions(),
                        pathItem.getHead(),
                        pathItem.getTrace())
                    .filter(Objects::nonNull)
                    .forEach(
                        operation -> {
                          if (operation.getTags() != null) {
                            usedTags.addAll(operation.getTags());
                          }
                        });
              });
    }

    if (openAPI.getTags() != null) {
      Set<String> definedTags = new HashSet<>();
      openAPI.getTags().forEach(tag -> definedTags.add(tag.getName()));

      usedTags.forEach(
          tag -> {
            assertTrue(
                definedTags.contains(tag),
                "Tag '" + tag + "' is used but not defined in the global tags section");
          });
    }
  }

  @Test
  public void testAspectPatchPropertySchema() {
    // Verify that AspectPatchProperty schema exists in components
    assertNotNull(
        openAPI.getComponents().getSchemas().get("AspectPatchProperty"),
        "AspectPatchProperty schema should be present in components");

    Schema aspectPatchPropertySchema =
        openAPI.getComponents().getSchemas().get("AspectPatchProperty");

    // Verify schema type
    assertEquals(
        aspectPatchPropertySchema.getType(),
        "object",
        "AspectPatchProperty should be of type object");

    // Verify required fields
    assertNotNull(
        aspectPatchPropertySchema.getRequired(), "AspectPatchProperty should have required fields");
    assertEquals(
        aspectPatchPropertySchema.getRequired().size(),
        1,
        "AspectPatchProperty should have exactly 1 required field");
    assertTrue(
        aspectPatchPropertySchema.getRequired().contains("value"),
        "AspectPatchProperty should require 'value' field");

    // Verify properties
    Map<String, Schema> properties = aspectPatchPropertySchema.getProperties();
    assertNotNull(properties, "AspectPatchProperty should have properties");
    assertEquals(properties.size(), 3, "AspectPatchProperty should have exactly 3 properties");

    // Verify 'value' property
    Schema valueProperty = properties.get("value");
    assertNotNull(valueProperty, "AspectPatchProperty should have 'value' property");
    assertEquals(
        valueProperty.get$ref(),
        "#/components/schemas/AspectPatch",
        "Value property should reference AspectPatch schema");
    assertEquals(
        valueProperty.getDescription(),
        "Patch to apply to the aspect.",
        "Value property should have correct description");

    // Verify 'systemMetadata' property
    Schema systemMetadataProperty = properties.get("systemMetadata");
    assertNotNull(systemMetadataProperty);

    assertNull(systemMetadataProperty.get$ref());
    assertEquals(systemMetadataProperty.getTypes(), Set.of("object", "null"));
    assertNotNull(systemMetadataProperty.getOneOf());
    assertEquals(systemMetadataProperty.getOneOf().size(), 2);

    boolean hasSysMetaRef =
        systemMetadataProperty.getOneOf().stream()
            .anyMatch(s -> "#/components/schemas/SystemMetadata".equals(((Schema<?>) s).get$ref()));
    boolean hasNullAlt =
        systemMetadataProperty.getOneOf().stream()
            .anyMatch(s -> "null".equals(((Schema<?>) s).getType()));

    assertTrue(hasSysMetaRef, "systemMetadata oneOf must contain SystemMetadata ref");
    assertTrue(hasNullAlt, "systemMetadata oneOf must contain null type");

    // Verify 'headers' property
    Schema headersProperty = properties.get("headers");
    assertNotNull(headersProperty, "AspectPatchProperty should have 'headers' property");
    assertEquals(
        headersProperty.getTypes(),
        Set.of("object", "nullable"),
        "Headers property should have correct types");
    assertTrue(headersProperty.getNullable(), "Headers property should be nullable");
    assertNotNull(
        headersProperty.getAdditionalProperties(),
        "Headers property should have additionalProperties");
    assertEquals(
        ((Schema) headersProperty.getAdditionalProperties()).getType(),
        "string",
        "Headers additionalProperties should be of type string");
    assertEquals(
        headersProperty.getDescription(),
        "System headers for the operation.",
        "Headers property should have correct description");
  }

  @Test
  public void testAspectPatchPropertySchemaUsageInAspectPatch() {
    // Test that AspectPatchProperty is used in entity patch schemas
    // Get a sample entity patch schema
    Schema datasetPatchSchema =
        openAPI.getComponents().getSchemas().get("DatasetEntityRequestPatch_v3");
    assertNotNull(datasetPatchSchema, "Dataset patch schema should exist");

    // Check that at least datasetProperties references AspectPatchProperty
    Map<String, Schema> properties = datasetPatchSchema.getProperties();
    Schema datasetProperties = properties.get("datasetProperties");
    assertNotNull(datasetProperties, "Expected datasetProperties on dataset entity");

    assertEquals(
        datasetProperties.get$ref(),
        "#/components/schemas/AspectPatchProperty",
        "datasetProperties patch schemas should reference AspectPatchProperty for aspect properties");
  }

  @Test
  public void testGenericEntityEndpointsExist() {
    Map<String, PathItem> paths = openAPI.getPaths();

    /* ------------------------------------------------------------------ */
    /*  1)  /openapi/v3/entity  (POST  |  PATCH)                          */
    /* ------------------------------------------------------------------ */
    assertTrue(paths.containsKey("/openapi/v3/entity/generic"), "collection path must exist");
    PathItem collection = paths.get("/openapi/v3/entity/generic");

    // ---------- POST (create / replace) ----------
    assertNotNull(collection.getPost(), "POST (create/upsert) must exist");
    Schema<?> postBodySchema =
        collection.getPost().getRequestBody().getContent().get("application/json").getSchema();
    assertEquals(
        postBodySchema.get$ref(),
        "#/components/schemas/CrossEntitiesRequest_v3",
        "collection POST must use CrossEntitiesRequest_v3");

    // ---------- PATCH (batch patch) --------------
    assertNotNull(collection.getPatch(), "PATCH (batch patch) must exist");
    Schema<?> patchBodySchema =
        collection.getPatch().getRequestBody().getContent().get("application/json").getSchema();
    assertEquals(
        patchBodySchema.get$ref(),
        "#/components/schemas/CrossEntitiesPatch_v3",
        "collection PATCH must use CrossEntitiesPatch_v3");

    /* ------------------------------------------------------------------ */
    /*  2)  /openapi/v3/entity/batchGet  (POST)                           */
    /* ------------------------------------------------------------------ */
    assertTrue(
        paths.containsKey("/openapi/v3/entity/generic/batchGet"), "batchGet path must exist");
    PathItem batchGet = paths.get("/openapi/v3/entity/generic/batchGet");
    assertNotNull(batchGet.getPost(), "batchGet must be a POST operation");

    Schema<?> batchGetBodySchema =
        batchGet.getPost().getRequestBody().getContent().get("application/json").getSchema();
    assertEquals(
        batchGetBodySchema.get$ref(),
        "#/components/schemas/CrossEntitiesBatchGetRequest_v3",
        "batchGet POST must use CrossEntitiesBatchGetRequest_v3");

    /* ------------------------------------------------------------------ */
    /*  3)  /openapi/v3/entity/{urn}  (GET | HEAD | DELETE)               */
    /* ------------------------------------------------------------------ */
    assertTrue(
        paths.containsKey("/openapi/v3/entity/generic/{urn}"), "single-entity path must exist");
    PathItem byUrn = paths.get("/openapi/v3/entity/generic/{urn}");
    assertNotNull(byUrn.getGet(), "GET by URN must exist");
    assertNotNull(byUrn.getHead(), "HEAD by URN must exist");
    assertNotNull(byUrn.getDelete(), "DELETE by URN must exist");

    /* ------------------------------------------------------------------ */
    /*  4)  /openapi/v3/entity/scroll  (POST – filter only, no aspects)   */
    /* ------------------------------------------------------------------ */
    assertTrue(paths.containsKey("/openapi/v3/entity/scroll"), "scroll path must exist");
    assertNotNull(paths.get("/openapi/v3/entity/scroll").getPost(), "scroll must be POST");

    // The scroll body is a filter and should use EntitiesEntityRequest_v3
    Schema<?> scrollSchema =
        paths
            .get("/openapi/v3/entity/scroll")
            .getPost()
            .getRequestBody()
            .getContent()
            .get("application/json")
            .getSchema();
    assertEquals(
        scrollSchema.get$ref(),
        "#/components/schemas/EntitiesEntityRequest_v3",
        "scroll body must use EntitiesEntityRequest_v3");

    /* ------------------------------------------------------------------ */
    /*  5)  Component presence quick-check                                */
    /* ------------------------------------------------------------------ */
    Set<String> componentKeys = openAPI.getComponents().getSchemas().keySet();
    assertTrue(componentKeys.contains("CrossEntitiesRequest_v3"));
    assertTrue(componentKeys.contains("CrossEntitiesPatch_v3"));
    assertTrue(componentKeys.contains("CrossEntitiesBatchGetRequest_v3"));
    assertTrue(componentKeys.contains("CrossEntitiesResponse_v3"));
  }

  @Test
  public void testCrossEntityArraysAreOptional() {
    String[] schemas = {"CrossEntitiesRequest_v3", "CrossEntitiesPatch_v3"};

    for (String schemaName : schemas) {
      Schema<?> schema = openAPI.getComponents().getSchemas().get(schemaName);
      assertNotNull(schema, "Component " + schemaName + " must exist");

      // 1)  No property except urn is required (required list null or empty)
      assertTrue(
          schema.getRequired() == null || schema.getRequired().isEmpty(),
          schemaName + " must not require any entity arrays");

      // 2)  Every property value is oneOf( array , null )
      schema
          .getProperties()
          .forEach(
              (propName, propSchema) -> {
                // Property schema should have no direct $ref and be wrapped in oneOf
                assertNull(
                    propSchema.get$ref(),
                    schemaName + "." + propName + " must not have direct $ref");
                assertNotNull(
                    propSchema.getOneOf(),
                    schemaName + "." + propName + " must be defined with oneOf");

                List<Schema<?>> oneOf = propSchema.getOneOf();
                assertEquals(oneOf.size(), 2, "oneOf must contain exactly two alternatives");

                boolean hasArray = oneOf.stream().anyMatch(s -> "array".equals(s.getType()));
                boolean hasNull = oneOf.stream().anyMatch(s -> "null".equals(s.getType()));

                assertTrue(
                    hasArray, schemaName + "." + propName + " oneOf must include array type");
                assertTrue(hasNull, schemaName + "." + propName + " oneOf must include null type");
              });
    }
  }

  private JsonSchema loadOpenAPI31Schema(JsonSchemaFactory schemaFactory) throws Exception {
    URL schemaUrl = new URL("https://spec.openapis.org/oas/3.1/schema/2022-10-07");
    return schemaFactory.getSchema(schemaUrl.openStream());
  }

  private void validateComponents(JsonNode components) {
    // Validate schemas if present
    if (components.has("schemas")) {
      JsonNode schemas = components.get("schemas");
      Iterator<Map.Entry<String, JsonNode>> schemaIterator = schemas.fields();
      while (schemaIterator.hasNext()) {
        Map.Entry<String, JsonNode> schemaEntry = schemaIterator.next();
        String schemaName = schemaEntry.getKey();
        JsonNode schema = schemaEntry.getValue();

        // Check for valid schema properties
        if (!schema.has("$ref")) {
          // If not a reference, should have type or other schema keywords
          boolean hasSchemaKeyword =
              schema.has("type")
                  || schema.has("properties")
                  || schema.has("items")
                  || schema.has("allOf")
                  || schema.has("oneOf")
                  || schema.has("anyOf")
                  || schema.has("not");

          assertTrue(
              hasSchemaKeyword,
              "Schema '" + schemaName + "' must have at least one schema keyword");
        }
      }
    }
  }

  private void resolveBatch(
      List<Map.Entry<String, PathItem>> batch, JsonNode rootNode, AtomicInteger resolvedCount) {
    // This validates and counts references in a batch of path items

    batch.forEach(
        entry -> {
          PathItem pathItem = entry.getValue();

          // Check PathItem reference
          if (pathItem.get$ref() != null) {
            if (isReferenceValid(pathItem.get$ref(), rootNode)) {
              resolvedCount.incrementAndGet();
            }
          }

          // Check operation references
          Stream.of(
                  pathItem.getGet(),
                  pathItem.getPost(),
                  pathItem.getPut(),
                  pathItem.getDelete(),
                  pathItem.getPatch(),
                  pathItem.getOptions(),
                  pathItem.getHead(),
                  pathItem.getTrace())
              .filter(Objects::nonNull)
              .forEach(
                  operation -> {
                    // Check request body
                    if (operation.getRequestBody() != null
                        && operation.getRequestBody().get$ref() != null) {
                      if (isReferenceValid(operation.getRequestBody().get$ref(), rootNode)) {
                        resolvedCount.incrementAndGet();
                      }
                    }

                    // Check responses
                    if (operation.getResponses() != null) {
                      operation
                          .getResponses()
                          .forEach(
                              (code, response) -> {
                                if (response.get$ref() != null) {
                                  if (isReferenceValid(response.get$ref(), rootNode)) {
                                    resolvedCount.incrementAndGet();
                                  }
                                }

                                // Check response content
                                if (response.getContent() != null) {
                                  response
                                      .getContent()
                                      .forEach(
                                          (mediaType, content) -> {
                                            if (content.getSchema() != null
                                                && content.getSchema().get$ref() != null) {
                                              if (isReferenceValid(
                                                  content.getSchema().get$ref(), rootNode)) {
                                                resolvedCount.incrementAndGet();
                                              }
                                            }
                                          });
                                }
                              });
                    }

                    // Check parameters
                    if (operation.getParameters() != null) {
                      operation
                          .getParameters()
                          .forEach(
                              param -> {
                                if (param.get$ref() != null) {
                                  if (isReferenceValid(param.get$ref(), rootNode)) {
                                    resolvedCount.incrementAndGet();
                                  }
                                }
                                if (param.getSchema() != null
                                    && param.getSchema().get$ref() != null) {
                                  if (isReferenceValid(param.getSchema().get$ref(), rootNode)) {
                                    resolvedCount.incrementAndGet();
                                  }
                                }
                              });
                    }
                  });
        });
  }

  private boolean isReferenceValid(String ref, JsonNode rootNode) {
    if (ref.startsWith("#/")) {
      String path = ref.substring(2);
      String[] parts = path.split("/");

      JsonNode current = rootNode;
      for (String part : parts) {
        current = current.get(part);
        if (current == null) {
          return false;
        }
      }
      return true;
    }
    // External references - for this test, consider them valid
    return true;
  }

  private void countAllReferences(OpenAPI spec, AtomicInteger count) {
    // Count references in paths
    if (spec.getPaths() != null) {
      spec.getPaths()
          .forEach(
              (path, pathItem) -> {
                countPathItemReferences(pathItem, count);
              });
    }

    // Count references in components
    if (spec.getComponents() != null) {
      if (spec.getComponents().getSchemas() != null) {
        spec.getComponents()
            .getSchemas()
            .forEach(
                (name, schema) -> {
                  countSchemaReferencesSimple(schema, count);
                });
      }
    }
  }

  private void countPathItemReferences(PathItem pathItem, AtomicInteger count) {
    if (pathItem.get$ref() != null) {
      count.incrementAndGet();
    }

    Stream.of(
            pathItem.getGet(),
            pathItem.getPost(),
            pathItem.getPut(),
            pathItem.getDelete(),
            pathItem.getPatch(),
            pathItem.getOptions(),
            pathItem.getHead(),
            pathItem.getTrace())
        .filter(Objects::nonNull)
        .forEach(
            operation -> {
              if (operation.getRequestBody() != null
                  && operation.getRequestBody().get$ref() != null) {
                count.incrementAndGet();
              }

              if (operation.getResponses() != null) {
                operation
                    .getResponses()
                    .forEach(
                        (code, response) -> {
                          if (response.get$ref() != null) {
                            count.incrementAndGet();
                          }
                          if (response.getContent() != null) {
                            response
                                .getContent()
                                .forEach(
                                    (mediaType, content) -> {
                                      if (content.getSchema() != null
                                          && content.getSchema().get$ref() != null) {
                                        count.incrementAndGet();
                                      }
                                    });
                          }
                        });
              }

              if (operation.getParameters() != null) {
                operation
                    .getParameters()
                    .forEach(
                        param -> {
                          if (param.get$ref() != null) {
                            count.incrementAndGet();
                          }
                          if (param.getSchema() != null && param.getSchema().get$ref() != null) {
                            count.incrementAndGet();
                          }
                        });
              }
            });
  }

  private void countSchemaReferencesSimple(Schema<?> schema, AtomicInteger count) {
    if (schema.get$ref() != null) {
      count.incrementAndGet();
    }

    if (schema.getProperties() != null) {
      schema
          .getProperties()
          .forEach(
              (propName, propSchema) -> {
                countSchemaReferencesSimple(propSchema, count);
              });
    }

    if (schema.getItems() != null) {
      countSchemaReferencesSimple(schema.getItems(), count);
    }

    Stream.of(schema.getAllOf(), schema.getOneOf(), schema.getAnyOf())
        .filter(Objects::nonNull)
        .flatMap(List::stream)
        .forEach(s -> countSchemaReferencesSimple(s, count));
  }

  private void countSchemaReferences(
      Schema<?> schema, JsonNode rootNode, AtomicInteger resolvedCount) {
    if (schema.get$ref() != null) {
      if (isReferenceValid(schema.get$ref(), rootNode)) {
        resolvedCount.incrementAndGet();
      }
    }

    if (schema.getProperties() != null) {
      schema
          .getProperties()
          .forEach(
              (propName, propSchema) -> {
                countSchemaReferences(propSchema, rootNode, resolvedCount);
              });
    }

    if (schema.getItems() != null) {
      countSchemaReferences(schema.getItems(), rootNode, resolvedCount);
    }

    Stream.of(schema.getAllOf(), schema.getOneOf(), schema.getAnyOf())
        .filter(Objects::nonNull)
        .flatMap(List::stream)
        .forEach(s -> countSchemaReferences(s, rootNode, resolvedCount));
  }
}
