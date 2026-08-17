package io.datahubproject.openapi.openlineage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import io.datahubproject.openapi.openlineage.config.PinnedOpenLineageOpenApiCustomizer;
import io.datahubproject.openapi.openlineage.validation.OpenLineageSchemaCatalog;
import io.datahubproject.openlineage.customfacet.CompatibilityFacetCatalog;
import io.datahubproject.openlineage.customfacet.CompatibilityFacetCatalog.AttachmentPoint;
import io.datahubproject.openlineage.customfacet.CompatibilityFacetContract;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.parser.OpenAPIV3Parser;
import io.swagger.v3.parser.core.models.SwaggerParseResult;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class OpenLineageOpenApiContractTest {
  private final ObjectMapper objectMapper = new ObjectMapper();
  private JsonNode contract;
  private JsonSchema requestSchema;
  private JsonSchema errorResponseSchema;

  @BeforeClass
  public void loadContract() throws Exception {
    URL resource = getClass().getResource("/openlineage/openlineage.json");
    assertNotNull(resource);
    contract = objectMapper.readTree(resource);
    ObjectNode schema =
        contract
            .at("/paths/~1lineage/post/requestBody/content/application~1json/schema")
            .deepCopy();
    schema.set("components", contract.get("components"));
    requestSchema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7).getSchema(schema);
    ObjectNode errorSchema = contract.at("/components/schemas/OpenLineageErrorResponse").deepCopy();
    errorSchema.set("components", contract.get("components"));
    errorResponseSchema =
        JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7).getSchema(errorSchema);
  }

  @Test
  public void testPinnedContractHasResolvedRootEventsAndStandardFacets() {
    URL resource = getClass().getResource("/openlineage/openlineage.json");
    assertNotNull(resource);

    SwaggerParseResult result = new OpenAPIV3Parser().readLocation(resource.toString(), null, null);
    assertTrue(
        result.getMessages() == null || result.getMessages().isEmpty(),
        String.valueOf(result.getMessages()));
    OpenAPI api = result.getOpenAPI();
    assertNotNull(api);
    List.of(
            "RunEvent",
            "JobEvent",
            "DatasetEvent",
            "NominalTimeRunFacet",
            "ErrorMessageRunFacet",
            "DocumentationJobFacet",
            "SchemaDatasetFacet",
            "ColumnLineageDatasetFacet",
            "DataQualityAssertionsDatasetFacet",
            "DataQualityMetricsInputDatasetFacet",
            "OutputStatisticsOutputDatasetFacet",
            "JobDependenciesRunFacet",
            "JobDependency",
            "HierarchyDatasetFacet",
            "HierarchyDatasetFacetLevel")
        .forEach(name -> assertNotNull(api.getComponents().getSchemas().get(name), name));
    assertTrue(api.getPaths().get("/lineage").getPost().getRequestBody().getRequired());
    assertNotNull(
        api.getPaths()
            .get("/lineage")
            .getPost()
            .getRequestBody()
            .getContent()
            .get("application/json")
            .getSchema()
            .getOneOf());
  }

  @Test
  public void testPinnedContractDocumentsStructuredClientErrors() {
    for (String status : List.of("400", "401", "403", "415")) {
      assertEquals(
          contract
              .at(
                  "/paths/~1lineage/post/responses/"
                      + status
                      + "/content/application~1json/schema/$ref")
              .asText(),
          "#/components/schemas/OpenLineageErrorResponse");
    }
  }

  @Test
  public void testLiveSpringdocCustomizerUsesThePinnedContract() {
    OpenAPI live = new OpenAPI();
    new PinnedOpenLineageOpenApiCustomizer().customise(live);

    assertNotNull(live.getPaths().get("/lineage"));
    assertEquals(live.getServers().get(0).getUrl(), "/openapi/openlineage/api/v1");
    assertEquals(
        live.getPaths()
            .get("/lineage")
            .getPost()
            .getRequestBody()
            .getContent()
            .get("application/json")
            .getSchema()
            .getOneOf()
            .size(),
        3);
    assertNotNull(live.getComponents().getSchemas().get("NominalTimeRunFacet"));
    assertNotNull(live.getComponents().getSchemas().get("OpenLineageErrorResponse"));
  }

  @Test
  public void testRepresentativeRootEventsMatchExactlyOneSchema() throws Exception {
    assertValid(
        baseFields("RunEvent")
            + ",\"eventType\":\"COMPLETE\","
            + "\"run\":{\"runId\":\"123e4567-e89b-12d3-a456-426614174000\"},"
            + "\"job\":{\"namespace\":\"crm\",\"name\":\"load.customer\"}}");
    assertValid(
        baseFields("JobEvent")
            + ",\"job\":{\"namespace\":\"crm\",\"name\":\"load.customer\"},"
            + "\"inputs\":[],\"outputs\":[]}");
    assertValid(
        baseFields("DatasetEvent")
            + ",\"dataset\":{\"namespace\":\"snowflake\",\"name\":\"db.schema.table\"}}");
  }

  @Test
  public void testAdditionalRootPropertiesFollowTheOfficialOneOf() throws Exception {
    assertInvalid(
        baseFields("JobEvent")
            + ",\"job\":{\"namespace\":\"crm\",\"name\":\"load.customer\"},"
            + "\"dataset\":{\"namespace\":\"snowflake\",\"name\":\"db.schema.table\"}}");
    assertValid(
        baseFields("RunEvent")
            + ",\"run\":{\"runId\":\"123e4567-e89b-12d3-a456-426614174000\"},"
            + "\"job\":{\"namespace\":\"crm\",\"name\":\"load.customer\"},"
            + "\"dataset\":{\"namespace\":\"snowflake\",\"name\":\"db.schema.table\"}}");
    assertValid(
        baseFields("DatasetEvent")
            + ",\"run\":{\"runId\":\"123e4567-e89b-12d3-a456-426614174000\"},"
            + "\"dataset\":{\"namespace\":\"snowflake\",\"name\":\"db.schema.table\"}}");
    assertValid(
        baseFields("JobEvent")
            + ",\"job\":{\"namespace\":\"crm\",\"name\":\"load.customer\"},"
            + "\"dataset\":null}");
    assertValid(
        baseFields("DatasetEvent")
            + ",\"dataset\":{\"namespace\":\"snowflake\",\"name\":\"db.schema.table\"},"
            + "\"job\":null}");
    assertInvalid(
        baseFields("JobEvent")
            + ",\"run\":null,"
            + "\"job\":{\"namespace\":\"crm\",\"name\":\"load.customer\"}}");
    assertInvalid("{}");
    assertInvalid("\"not-an-event\"");
  }

  @Test
  public void testTypedJobDependenciesAndHierarchyFacetsAreAccepted() throws Exception {
    String producer = "\"_producer\":\"https://example.com/my-pipeline-tool\",";
    String jobDependenciesIdentity =
        "\"_schemaURL\":\"https://openlineage.io/spec/facets/1-0-1/JobDependenciesRunFacet.json\"";
    String hierarchyIdentity =
        "\"_schemaURL\":\"https://openlineage.io/spec/facets/1-0-0/HierarchyDatasetFacet.json\"";
    assertValid(
        baseFields("RunEvent")
            + ",\"eventType\":\"COMPLETE\","
            + "\"run\":{\"runId\":\"123e4567-e89b-12d3-a456-426614174000\","
            + "\"facets\":{\"jobDependencies\":{"
            + producer
            + jobDependenciesIdentity
            + ",\"upstream\":[{\"job\":{\"namespace\":\"crm\",\"name\":\"extract.customer\"},"
            + "\"dependency_type\":\"DIRECT_INVOCATION\"}],\"trigger_rule\":\"ALL_SUCCESS\"}}},"
            + "\"job\":{\"namespace\":\"crm\",\"name\":\"load.customer\"}}");
    assertValid(
        baseFields("DatasetEvent")
            + ",\"dataset\":{\"namespace\":\"snowflake\",\"name\":\"analytics.sales.orders\","
            + "\"facets\":{\"hierarchy\":{"
            + producer
            + hierarchyIdentity
            + ",\"hierarchy\":[{\"type\":\"DATABASE\",\"name\":\"analytics\"},"
            + "{\"type\":\"SCHEMA\",\"name\":\"sales\"},"
            + "{\"type\":\"TABLE\",\"name\":\"orders\"}]}}}}");
    assertInvalid(
        baseFields("DatasetEvent")
            + ",\"dataset\":{\"namespace\":\"snowflake\",\"name\":\"analytics.sales.orders\","
            + "\"facets\":{\"hierarchy\":{"
            + producer
            + hierarchyIdentity
            + ",\"hierarchy\":[{\"type\":\"DATABASE\"}]}}}}");
  }

  @Test
  public void testRuntimeAndOpenApiFacetKeySetsMatch() {
    OpenLineageSchemaCatalog catalog = new OpenLineageSchemaCatalog();
    Set<String> compatibilityKeys =
        CompatibilityFacetCatalog.contracts().stream()
            .map(CompatibilityFacetContract::key)
            .collect(Collectors.toSet());
    assertEquals(
        propertyNames("/components/schemas/Run/properties/facets/properties"),
        union(catalog.standardFacetKeys(AttachmentPoint.RUN), compatibilityKeys));
    assertEquals(
        propertyNames("/components/schemas/Job/properties/facets/properties"),
        catalog.standardFacetKeys(AttachmentPoint.JOB));
    assertEquals(
        propertyNames("/components/schemas/Dataset/properties/facets/properties"),
        catalog.standardFacetKeys(AttachmentPoint.DATASET));
    assertEquals(
        propertyNames("/components/schemas/InputDataset/allOf/1/properties/inputFacets/properties"),
        catalog.standardFacetKeys(AttachmentPoint.INPUT_DATASET));
    assertEquals(
        propertyNames(
            "/components/schemas/OutputDataset/allOf/1/properties/outputFacets/properties"),
        catalog.standardFacetKeys(AttachmentPoint.OUTPUT_DATASET));
  }

  @Test
  public void testCompatibleSchemaMetadataMatchesRuntimePolicy() throws Exception {
    String validJob =
        baseFields("JobEvent") + ",\"job\":{\"namespace\":\"crm\",\"name\":\"load.customer\"}}";
    assertValid(
        validJob.replace(
            "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobEvent",
            "https://openlineage.io/spec/2-0-2/OpenLineage.json"));
    assertValid(validJob.replaceFirst(",\"schemaURL\":\"[^\"]+\"", ""));

    String validNominalTime =
        baseFields("RunEvent")
            + ",\"eventType\":\"START\","
            + "\"run\":{\"runId\":\"producer-run-id\","
            + "\"facets\":{\"nominalTime\":{"
            + "\"nominalStartTime\":\"2026-04-14T10:00:00\"}}},"
            + "\"job\":{\"namespace\":\"crm\",\"name\":\"load.customer\"}}";
    assertValid(validNominalTime);
    assertValid(
        validNominalTime.replace(
            "\"nominalStartTime\"",
            "\"_schemaURL\":\"https://example.com/alternate-facet.json\",\"nominalStartTime\""));
  }

  @Test
  public void testCustomCompatibilityFacetsAreOpaqueAtTheRequestBoundary() throws Exception {
    String matching =
        runWithFacet(
            "\"spark_jobDetails\":{"
                + "\"_producer\":\"https://github.com/OpenLineage/OpenLineage/tree/1.45.0/integration/spark\","
                + "\"_schemaURL\":\"https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunFacet\","
                + "\"jobId\":42}");
    assertValid(matching);
    assertValid(matching.replace("\"jobId\":42", "\"jobId\":\"not-an-integer\""));
    assertValid(
        matching
            .replace(
                "https://github.com/OpenLineage/OpenLineage/tree/1.45.0/integration/spark",
                "https://example.com/not-spark")
            .replace("\"jobId\":42", "\"jobId\":\"opaque\""));
    assertValid(
        matching
            .replace(
                "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunFacet",
                "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/BaseFacet")
            .replace("\"jobId\":42", "\"jobId\":\"opaque\""));

    assertValid(
        runWithFacet(
            "\"airflow\":{"
                + "\"_producer\":\"https://github.com/apache/airflow/tree/providers-openlineage/2.3.0\","
                + "\"_schemaURL\":\"https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/BaseFacet\","
                + "\"dag\":{\"dag_id\":\"daily\",\"tags\":\"['prod']\"}}"));
  }

  @Test
  public void testErrorDetailsMatchInvalidAuthenticationAndIngestionResponses() throws Exception {
    assertErrorResponseValid(
        "{\"code\":\"INVALID_EVENT\",\"message\":\"Invalid OpenLineage event\","
            + "\"details\":{\"errors\":[{\"path\":\"$\",\"rule\":\"required\"}]}}");
    assertErrorResponseValid(
        "{\"code\":\"AUTHENTICATION_REQUIRED\",\"message\":\"Authentication required\","
            + "\"details\":{}}");
    assertErrorResponseValid(
        "{\"code\":\"INGESTION_FAILED\",\"message\":\"Ingestion failed\","
            + "\"details\":{\"exception\":\"IllegalStateException\"}}");
    assertErrorResponseValid(
        "{\"code\":\"UNSUPPORTED_MEDIA_TYPE\",\"message\":\"Unsupported media type\","
            + "\"details\":{}}");
  }

  @Test
  public void testDataQualityAssertionsIsAnInputFacetOnly() {
    JsonNode inputFacets =
        contract.at(
            "/components/schemas/InputDataset/allOf/1/properties/inputFacets/properties/dataQualityAssertions");
    JsonNode datasetFacets =
        contract.at(
            "/components/schemas/Dataset/properties/facets/properties/dataQualityAssertions");
    assertFalse(inputFacets.isMissingNode());
    assertTrue(datasetFacets.isMissingNode());
  }

  private Set<String> propertyNames(String path) {
    Set<String> names = new HashSet<>();
    contract.at(path).fieldNames().forEachRemaining(names::add);
    return names;
  }

  private static Set<String> union(Set<String> left, Set<String> right) {
    Set<String> values = new HashSet<>(left);
    values.addAll(right);
    return values;
  }

  private void assertValid(String json) throws Exception {
    assertTrue(requestSchema.validate(objectMapper.readTree(json)).isEmpty());
  }

  private void assertInvalid(String json) throws Exception {
    assertFalse(requestSchema.validate(objectMapper.readTree(json)).isEmpty());
  }

  private void assertErrorResponseValid(String json) throws Exception {
    assertTrue(errorResponseSchema.validate(objectMapper.readTree(json)).isEmpty());
  }

  private static String runWithFacet(String facet) {
    return baseFields("RunEvent")
        + ",\"eventType\":\"START\","
        + "\"run\":{\"runId\":\"123e4567-e89b-12d3-a456-426614174000\","
        + "\"facets\":{"
        + facet
        + "}},"
        + "\"job\":{\"namespace\":\"crm\",\"name\":\"load.customer\"}}";
  }

  private static String baseFields(String eventType) {
    return "{"
        + "\"eventTime\":\"2026-04-14T10:00:00Z\","
        + "\"producer\":\"https://example.com/my-pipeline-tool\","
        + "\"schemaURL\":\"https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/"
        + eventType
        + "\"";
  }
}
