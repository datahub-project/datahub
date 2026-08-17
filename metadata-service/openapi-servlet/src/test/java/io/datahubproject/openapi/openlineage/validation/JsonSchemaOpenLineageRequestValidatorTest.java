package io.datahubproject.openapi.openlineage.validation;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.datahubproject.openapi.openlineage.exception.InvalidOpenLineageEventException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class JsonSchemaOpenLineageRequestValidatorTest {
  private static final String ROOT_SCHEMA = "https://openlineage.io/spec/2-0-2/OpenLineage.json";
  private static final String RUN_FACET_SCHEMA = ROOT_SCHEMA + "#/$defs/RunFacet";
  private static final String PRODUCER = "https://example.com/lineage-producer";
  private static final String SPARK_PRODUCER =
      "https://github.com/OpenLineage/OpenLineage/tree/1.45.0/integration/spark";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private JsonSchemaOpenLineageRequestValidator validator;

  @BeforeClass
  public void setup() {
    validator = new JsonSchemaOpenLineageRequestValidator(new OpenLineageSchemaCatalog());
  }

  @Test
  public void testAcceptsAllOfficialRootEventTypes() {
    assertEquals(validate(validRunEvent()).path("run").path("runId").asText(), runId());
    assertEquals(validate(validJobEvent()).path("job").path("name").asText(), "load.customer");
    assertEquals(
        validate(validDatasetEvent()).path("dataset").path("name").asText(), "analytics.customer");
  }

  @Test
  public void testRejectsDuplicateKeysAndTrailingContentBeforeBinding() {
    assertError(
        validJobEvent()
            .replace(
                "\"producer\":\"" + PRODUCER + "\"",
                "\"producer\":\"" + PRODUCER + "\",\"producer\":\"https://evil.example\""),
        "$",
        "duplicateKey");
    assertError(validJobEvent() + "{}", "$", "trailingContent");
  }

  @Test
  public void testDispatchesByShapeAndAcceptsCompatibleSchemaMetadata() {
    validate(validJobEvent().replace(ROOT_SCHEMA + "#/$defs/JobEvent", ROOT_SCHEMA));
    validate(
        validJobEvent()
            .replace(
                ROOT_SCHEMA + "#/$defs/JobEvent",
                "https://vendor.example/openlineage/job-event-schema.json"));
    validate(validJobEvent().replace(",\"schemaURL\":\"" + ROOT_SCHEMA + "#/$defs/JobEvent\"", ""));
    validate(
        validRunEvent().substring(0, validRunEvent().length() - 1)
            + ",\"dataset\":{\"namespace\":\"snowflake\",\"name\":\"db.table\"}}");
    validate(
        validDatasetEvent().substring(0, validDatasetEvent().length() - 1)
            + ",\"run\":{\"runId\":\""
            + runId()
            + "\"}}");
    validate(validJobEvent().substring(0, validJobEvent().length() - 1) + ",\"dataset\":null}");
    validate(validDatasetEvent().substring(0, validDatasetEvent().length() - 1) + ",\"job\":null}");

    assertError(
        validJobEvent().substring(0, validJobEvent().length() - 1)
            + ",\"dataset\":{\"namespace\":\"snowflake\",\"name\":\"db.table\"}}",
        "$",
        "oneOf");
    assertError(
        validJobEvent().substring(0, validJobEvent().length() - 1) + ",\"run\":null}",
        "$",
        "oneOf");
  }

  @DataProvider
  public Object[][] invalidTypedValues() {
    return new Object[][] {
      {validRunEvent().replace("\"runId\":\"" + runId() + "\"", "\"runId\":42"), "$.run.runId"},
      {
        validJobEvent().replace("\"producer\":\"" + PRODUCER + "\"", "\"producer\":42"),
        "$.producer"
      },
      {validJobEvent().replace("\"name\":\"load.customer\"", "\"name\":42"), "$.job.name"},
      {
        validRunEvent().replace("\"eventType\":\"START\"", "\"eventType\":\"UNKNOWN\""),
        "$.eventType"
      }
    };
  }

  @Test(dataProvider = "invalidTypedValues")
  public void testEnforcesRequiredFieldTypesAndEnums(String event, String path) {
    assertErrorAtPath(event, path);
  }

  @Test
  public void testStandardFacetsUseAttachmentAndKeyRatherThanSchemaIdentity() {
    String validFacet =
        "{\"_producer\":\""
            + PRODUCER
            + "\",\"_schemaURL\":\"https://openlineage.io/spec/facets/1-0-1/NominalTimeRunFacet.json\","
            + "\"nominalStartTime\":\"2026-04-14T10:00:00Z\"}";
    validate(withRunFacets("\"nominalTime\":" + validFacet));
    validate(
        withRunFacets(
            "\"nominalTime\":"
                + validFacet.replace(
                    "NominalTimeRunFacet.json", "OpenLineage.json#/$defs/RunFacet")));
    validate(withRunFacets("\"nominalTime\":{\"nominalStartTime\":\"2026-04-14T10:00:00Z\"}"));

    String wrongAttachment =
        validJobEvent()
            .replace(
                "\"job\":{\"namespace\":\"crm\",\"name\":\"load.customer\"}",
                "\"job\":{\"namespace\":\"crm\",\"name\":\"load.customer\","
                    + "\"facets\":{\"nominalTime\":"
                    + validFacet
                    + "}}");
    assertError(wrongAttachment, "$.job.facets.nominalTime", "attachment");
  }

  @Test
  public void testRejectsStandardFacetsInWrongInputAndOutputFacetMaps() {
    String outputFacetOnInput =
        validJobEvent()
            .replace(
                "\"inputs\":[]",
                "\"inputs\":[{\"namespace\":\"snowflake\",\"name\":\"db.input\","
                    + "\"outputFacets\":{\"outputStatistics\":{\"rowCount\":1}}}]");
    assertError(outputFacetOnInput, "$.inputs[0].outputFacets.outputStatistics", "attachment");

    String inputFacetOnOutput =
        validJobEvent()
            .replace(
                "\"outputs\":[]",
                "\"outputs\":[{\"namespace\":\"snowflake\",\"name\":\"db.output\","
                    + "\"inputFacets\":{\"dataQualityMetrics\":{\"columnMetrics\":{}}}}]");
    assertError(inputFacetOnOutput, "$.outputs[0].inputFacets.dataQualityMetrics", "attachment");
  }

  @Test
  public void testKnownStandardFacetsEnforceConsumedFieldTypes() {
    assertErrorAtPath(
        withRunFacets("\"nominalTime\":{\"nominalStartTime\":42}"),
        "$.run.facets.nominalTime.nominalStartTime");
    assertErrorAtPath(withJobFacets("\"sql\":{\"query\":42}"), "$.job.facets.sql.query");
  }

  @Test
  public void testKnownStandardFacetsEnforceRequiredConsumedFields() {
    assertError(withRunFacets("\"nominalTime\":{}"), "$.run.facets.nominalTime", "required");
    assertError(withJobFacets("\"sql\":{}"), "$.job.facets.sql", "required");
  }

  @Test
  public void testSchemaMetadataMustBeAbsoluteUrisWhenPresent() {
    assertError(
        validJobEvent().replace(ROOT_SCHEMA + "#/$defs/JobEvent", "relative-schema.json"),
        "$.schemaURL",
        "format");
    assertError(
        withRunFacets("\"vendor_secret\":{\"_schemaURL\":\"not an absolute URI\",\"value\":42}"),
        "$.run.facets.vendor_secret._schemaURL",
        "format");
  }

  @Test
  public void testCustomCompatibilityFacetsRemainOpaque() {
    validate(
        withRunFacets(
            "\"spark_jobDetails\":{\"_producer\":\""
                + SPARK_PRODUCER
                + "\",\"_schemaURL\":\""
                + RUN_FACET_SCHEMA
                + "\",\"jobId\":\"producer-specific-id\",\"extension\":{\"enabled\":true}}"));
    validate(
        withRunFacets(
            "\"airflow\":{\"dag\":{\"dag_id\":\"daily\",\"tags\":[\"prod\"]},"
                + "\"producerSpecificField\":42}"));
  }

  @Test(timeOut = 2000)
  public void testUnknownFacetSchemaUrlsAreNotResolved() {
    validate(
        withRunFacets(
            "\"vendor_secret\":{\"_producer\":\""
                + PRODUCER
                + "\",\"_schemaURL\":\"http://192.0.2.1:9/never-resolve.json\","
                + "\"secret\":\"must-not-appear-in-errors\"}"));
    validate(
        withRunFacets(
            "\"vendor_secret\":{\"_producer\":\""
                + PRODUCER
                + "\",\"secret\":\"must-not-appear-in-errors\"}"));
    assertErrorAtPath(
        withRunFacets("\"vendor_secret\":\"not-an-object\""), "$.run.facets.vendor_secret");
  }

  @Test(timeOut = 5000)
  public void testFacetTypeErrorsStopAtTheBound() throws Exception {
    ObjectNode event = (ObjectNode) OBJECT_MAPPER.readTree(validJobEvent());
    ArrayNode inputs = event.putArray("inputs");
    for (int index = 0; index < 1000; index++) {
      ObjectNode input = inputs.addObject();
      input.put("namespace", "snowflake");
      input.put("name", "db.schema.table_" + index);
      input.putObject("facets").put("vendor_" + index, "not-an-object");
    }

    InvalidOpenLineageEventException exception = invalid(event.toString());
    assertEquals(exception.getValidationErrors().size(), 50);
    assertTrue(
        exception.getValidationErrors().stream().allMatch(error -> "type".equals(error.rule())));
  }

  @Test
  public void testErrorsAreDeterministicBoundedSortedAndValueFree() throws Exception {
    ObjectNode event = (ObjectNode) OBJECT_MAPPER.readTree(validRunEvent());
    ObjectNode facets = event.withObject("/run/facets");
    for (int index = 99; index >= 0; index--) {
      facets.put(String.format("vendor_%03d", index), "do-not-echo-this-value");
    }

    InvalidOpenLineageEventException first = invalid(event.toString());
    InvalidOpenLineageEventException second = invalid(event.toString());
    ObjectNode reorderedEvent = (ObjectNode) OBJECT_MAPPER.readTree(validRunEvent());
    ObjectNode reorderedFacets = reorderedEvent.withObject("/run/facets");
    for (int index = 0; index < 100; index++) {
      reorderedFacets.put(String.format("vendor_%03d", index), "a-different-sensitive-value");
    }
    InvalidOpenLineageEventException reordered = invalid(reorderedEvent.toString());

    List<OpenLineageValidationError> firstErrors = first.getValidationErrors();
    assertTrue(firstErrors.size() <= 50);
    assertEquals(firstErrors, second.getValidationErrors());
    assertEquals(firstErrors, reordered.getValidationErrors());

    List<OpenLineageValidationError> sorted = new ArrayList<>(firstErrors);
    sorted.sort(
        Comparator.comparing(OpenLineageValidationError::path)
            .thenComparing(
                OpenLineageValidationError::attachment, Comparator.nullsFirst(String::compareTo))
            .thenComparing(
                OpenLineageValidationError::facet, Comparator.nullsFirst(String::compareTo))
            .thenComparing(OpenLineageValidationError::rule));
    assertEquals(firstErrors, sorted);
    assertFalse(OBJECT_MAPPER.writeValueAsString(firstErrors).contains("do-not-echo-this-value"));
  }

  private JsonNode validate(String event) {
    return validator.validate(event.getBytes(StandardCharsets.UTF_8));
  }

  private InvalidOpenLineageEventException invalid(String event) {
    return expectThrows(InvalidOpenLineageEventException.class, () -> validate(event));
  }

  private void assertError(String event, String path, String rule) {
    InvalidOpenLineageEventException exception = invalid(event);
    assertTrue(
        exception.getValidationErrors().stream()
            .anyMatch(error -> path.equals(error.path()) && rule.equals(error.rule())),
        exception.getValidationErrors().toString());
  }

  private void assertErrorAtPath(String event, String path) {
    InvalidOpenLineageEventException exception = invalid(event);
    assertTrue(
        exception.getValidationErrors().stream().anyMatch(error -> path.equals(error.path())),
        exception.getValidationErrors().toString());
  }

  private static String validRunEvent() {
    return "{"
        + baseFields("RunEvent")
        + ",\"eventType\":\"START\","
        + "\"run\":{\"runId\":\""
        + runId()
        + "\"},"
        + "\"job\":{\"namespace\":\"crm\",\"name\":\"load.customer\"}}";
  }

  private static String validJobEvent() {
    return "{"
        + baseFields("JobEvent")
        + ",\"job\":{\"namespace\":\"crm\",\"name\":\"load.customer\"},"
        + "\"inputs\":[],\"outputs\":[]}";
  }

  private static String validDatasetEvent() {
    return "{"
        + baseFields("DatasetEvent")
        + ",\"dataset\":{\"namespace\":\"snowflake\",\"name\":\"analytics.customer\"}}";
  }

  private static String withRunFacets(String facets) {
    return validRunEvent()
        .replace(
            "\"run\":{\"runId\":\"" + runId() + "\"}",
            "\"run\":{\"runId\":\"" + runId() + "\",\"facets\":{" + facets + "}}");
  }

  private static String withJobFacets(String facets) {
    return validJobEvent()
        .replace(
            "\"job\":{\"namespace\":\"crm\",\"name\":\"load.customer\"}",
            "\"job\":{\"namespace\":\"crm\",\"name\":\"load.customer\",\"facets\":{"
                + facets
                + "}}");
  }

  private static String baseFields(String eventType) {
    return "\"eventTime\":\"2026-04-14T10:00:00Z\","
        + "\"producer\":\""
        + PRODUCER
        + "\","
        + "\"schemaURL\":\""
        + ROOT_SCHEMA
        + "#/$defs/"
        + eventType
        + "\"";
  }

  private static String runId() {
    return "123e4567-e89b-12d3-a456-426614174000";
  }
}
