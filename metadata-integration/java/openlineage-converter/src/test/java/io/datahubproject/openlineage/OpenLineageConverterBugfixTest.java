package io.datahubproject.openlineage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.FabricType;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.SchemaMetadata;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.converter.OpenLineageToDataHub;
import io.datahubproject.openlineage.dataset.DatahubJob;
import io.openlineage.client.OpenLineage;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.testng.annotations.Test;

/**
 * Regression tests for two OpenLineage converter bugs:
 *
 * <ul>
 *   <li>Schema field type mapping was case-sensitive, so uppercase type names (common from
 *       Trino/JDBC producers) fell through to NullType.
 *   <li>{@code eventType = OTHER} produced a {@code RunResultType.$UNKNOWN} aspect that fails
 *       downstream validation (silent drop), and a missing {@code eventType} NPE'd.
 * </ul>
 */
public class OpenLineageConverterBugfixTest {

  private static final URI PRODUCER = URI.create("https://example.com/my-pipeline-tool");

  private static DatahubOpenlineageConfig config() {
    return DatahubOpenlineageConfig.builder()
        .fabricType(FabricType.PROD)
        .orchestrator("spark")
        .build();
  }

  // ---- Schema field type case-insensitivity ----

  @Test
  public void schemaTypeMappingIsCaseInsensitive() {
    // Uppercase variants (Trino/JDBC producers) must resolve the same as lowercase.
    assertTrue(
        OpenLineageToDataHub.convertOlFieldTypeToDHFieldType("STRING").isStringType(),
        "'STRING' should map to StringType");
    assertTrue(
        OpenLineageToDataHub.convertOlFieldTypeToDHFieldType("INT").isNumberType(),
        "'INT' should map to NumberType");
    assertTrue(
        OpenLineageToDataHub.convertOlFieldTypeToDHFieldType("LONG").isNumberType(),
        "'LONG' should map to NumberType");
    assertTrue(
        OpenLineageToDataHub.convertOlFieldTypeToDHFieldType("Timestamp").isTimeType(),
        "'Timestamp' should map to TimeType");
    assertTrue(
        OpenLineageToDataHub.convertOlFieldTypeToDHFieldType("STRUCT").isMapType(),
        "'STRUCT' should map to MapType");
    // Lowercase must keep working.
    assertTrue(OpenLineageToDataHub.convertOlFieldTypeToDHFieldType("string").isStringType());
  }

  @Test
  public void schemaTypeMappingCoversCommonSqlTypes() {
    // synonyms + parameterized suffixes resolve instead of falling to NullType
    assertTrue(OpenLineageToDataHub.convertOlFieldTypeToDHFieldType("varchar").isStringType());
    assertTrue(OpenLineageToDataHub.convertOlFieldTypeToDHFieldType("varchar(255)").isStringType());
    assertTrue(OpenLineageToDataHub.convertOlFieldTypeToDHFieldType("integer").isNumberType());
    assertTrue(OpenLineageToDataHub.convertOlFieldTypeToDHFieldType("bigint").isNumberType());
    assertTrue(
        OpenLineageToDataHub.convertOlFieldTypeToDHFieldType("decimal(10,2)").isNumberType());
    assertTrue(OpenLineageToDataHub.convertOlFieldTypeToDHFieldType("boolean").isBooleanType());
    assertTrue(OpenLineageToDataHub.convertOlFieldTypeToDHFieldType("date").isDateType());
    assertTrue(OpenLineageToDataHub.convertOlFieldTypeToDHFieldType("binary").isBytesType());
    assertTrue(OpenLineageToDataHub.convertOlFieldTypeToDHFieldType("array<string>").isArrayType());
    assertTrue(
        OpenLineageToDataHub.convertOlFieldTypeToDHFieldType("map<string,long>").isMapType());
    // genuinely unknown types still fall back to NullType
    assertTrue(
        OpenLineageToDataHub.convertOlFieldTypeToDHFieldType("some_weird_type").isNullType());
  }

  @Test
  public void schemaMetadataIncludesNestedStructFields() {
    OpenLineage ol = new OpenLineage(PRODUCER);
    OpenLineage.SchemaDatasetFacetFields city =
        ol.newSchemaDatasetFacetFieldsBuilder().name("city").type("string").build();
    OpenLineage.SchemaDatasetFacetFields zip =
        ol.newSchemaDatasetFacetFieldsBuilder().name("zip").type("long").build();
    OpenLineage.SchemaDatasetFacetFields address =
        ol.newSchemaDatasetFacetFieldsBuilder()
            .name("address")
            .type("struct")
            .fields(java.util.Arrays.asList(city, zip))
            .build();
    OpenLineage.SchemaDatasetFacet schema =
        ol.newSchemaDatasetFacetBuilder().fields(Collections.singletonList(address)).build();
    OpenLineage.InputDataset ds =
        ol.newInputDatasetBuilder()
            .namespace("s3://bucket")
            .name("db.table")
            .facets(ol.newDatasetFacetsBuilder().schema(schema).build())
            .build();

    SchemaMetadata md = OpenLineageToDataHub.getSchemaMetadata(ds, config());
    java.util.Set<String> paths =
        md.getFields().stream()
            .map(f -> f.getFieldPath())
            .collect(java.util.stream.Collectors.toSet());
    assertTrue(paths.contains("address"), "top-level struct field should be present: " + paths);
    assertTrue(
        paths.contains("address.city"), "nested field 'address.city' should be present: " + paths);
    assertTrue(
        paths.contains("address.zip"), "nested field 'address.zip' should be present: " + paths);
  }

  @Test
  public void schemaWithArrayEmitsV2FieldPaths() {
    OpenLineage ol = new OpenLineage(PRODUCER);
    OpenLineage.SchemaDatasetFacetFields id =
        ol.newSchemaDatasetFacetFieldsBuilder().name("id").type("long").build();
    // array of primitive: element type parsed from the type string.
    OpenLineage.SchemaDatasetFacetFields tags =
        ol.newSchemaDatasetFacetFieldsBuilder().name("tags").type("array<string>").build();
    // array of struct: element is a struct, its fields come via getFields().
    OpenLineage.SchemaDatasetFacetFields markerName =
        ol.newSchemaDatasetFacetFieldsBuilder().name("name").type("string").build();
    OpenLineage.SchemaDatasetFacetFields markers =
        ol.newSchemaDatasetFacetFieldsBuilder()
            .name("markers")
            .type("array")
            .fields(java.util.Collections.singletonList(markerName))
            .build();
    OpenLineage.SchemaDatasetFacet schema =
        ol.newSchemaDatasetFacetBuilder()
            .fields(java.util.Arrays.asList(id, tags, markers))
            .build();
    OpenLineage.InputDataset ds =
        ol.newInputDatasetBuilder()
            .namespace("s3://bucket")
            .name("db.table")
            .facets(ol.newDatasetFacetsBuilder().schema(schema).build())
            .build();

    java.util.Set<String> paths =
        OpenLineageToDataHub.getSchemaMetadata(ds, config()).getFields().stream()
            .map(f -> f.getFieldPath())
            .collect(java.util.stream.Collectors.toSet());

    // An array is present, so the whole schema must use v2 fieldPaths.
    assertTrue(
        paths.contains("[version=2.0].[type=struct].[type=long].id"), "simple field v2: " + paths);
    assertTrue(
        paths.contains("[version=2.0].[type=struct].[type=array].[type=string].tags"),
        "array<string> v2: " + paths);
    assertTrue(
        paths.contains("[version=2.0].[type=struct].[type=array].[type=struct].markers"),
        "array<struct> v2: " + paths);
    assertTrue(
        paths.contains(
            "[version=2.0].[type=struct].[type=array].[type=struct].markers.[type=string].name"),
        "nested field under array<struct> v2: " + paths);
  }

  @Test
  public void v2FieldPathsUseCanonicalTypeTokens() {
    OpenLineage ol = new OpenLineage(PRODUCER);
    // An array anywhere in the schema forces v2 fieldPaths for every field, so the parameterized
    // siblings below must still produce spec-conformant [type=...] tokens.
    OpenLineage.SchemaDatasetFacetFields tags =
        ol.newSchemaDatasetFacetFieldsBuilder().name("tags").type("array<string>").build();
    OpenLineage.SchemaDatasetFacetFields name =
        ol.newSchemaDatasetFacetFieldsBuilder().name("name").type("varchar(255)").build();
    OpenLineage.SchemaDatasetFacetFields amount =
        ol.newSchemaDatasetFacetFieldsBuilder().name("amount").type("decimal(10,2)").build();
    OpenLineage.SchemaDatasetFacet schema =
        ol.newSchemaDatasetFacetBuilder()
            .fields(java.util.Arrays.asList(tags, name, amount))
            .build();
    OpenLineage.InputDataset ds =
        ol.newInputDatasetBuilder()
            .namespace("s3://bucket")
            .name("db.table")
            .facets(ol.newDatasetFacetsBuilder().schema(schema).build())
            .build();

    java.util.Set<String> paths =
        OpenLineageToDataHub.getSchemaMetadata(ds, config()).getFields().stream()
            .map(f -> f.getFieldPath())
            .collect(java.util.stream.Collectors.toSet());

    assertTrue(
        paths.contains("[version=2.0].[type=struct].[type=string].name"),
        "varchar(255) should canonicalize to [type=string]: " + paths);
    assertTrue(
        paths.contains("[version=2.0].[type=struct].[type=decimal].amount"),
        "decimal(10,2) should drop its parameters: " + paths);
  }

  @Test
  public void v2ContainerElementTypesSurviveCommasInParameters() {
    OpenLineage ol = new OpenLineage(PRODUCER);
    // Splitting the angle-bracket payload naively on ',' breaks decimal(10,2) into "decimal(10".
    OpenLineage.SchemaDatasetFacetFields prices =
        ol.newSchemaDatasetFacetFieldsBuilder().name("prices").type("array<decimal(10,2)>").build();
    OpenLineage.SchemaDatasetFacetFields rates =
        ol.newSchemaDatasetFacetFieldsBuilder()
            .name("rates")
            .type("map<string,decimal(10,2)>")
            .build();
    OpenLineage.SchemaDatasetFacet schema =
        ol.newSchemaDatasetFacetBuilder().fields(java.util.Arrays.asList(prices, rates)).build();
    OpenLineage.InputDataset ds =
        ol.newInputDatasetBuilder()
            .namespace("s3://bucket")
            .name("db.table")
            .facets(ol.newDatasetFacetsBuilder().schema(schema).build())
            .build();

    java.util.Set<String> paths =
        OpenLineageToDataHub.getSchemaMetadata(ds, config()).getFields().stream()
            .map(f -> f.getFieldPath())
            .collect(java.util.stream.Collectors.toSet());

    assertTrue(
        paths.contains("[version=2.0].[type=struct].[type=array].[type=decimal].prices"),
        "array<decimal(10,2)> element type should be decimal: " + paths);
    assertTrue(
        paths.contains("[version=2.0].[type=struct].[type=map].[type=decimal].rates"),
        "map<string,decimal(10,2)> value type should be decimal: " + paths);
  }

  @Test
  public void v2NestedContainerTypesKeepTheFullTypeChain() {
    OpenLineage ol = new OpenLineage(PRODUCER);
    // field-path-spec-v2 encodes a nested container as the full chain of [type=...] tokens
    // (e.g. array-of-array-of-long -> [type=array].[type=array].[type=long]), so the innermost
    // element type must survive rather than collapsing to the outer container's token.
    OpenLineage.SchemaDatasetFacetFields matrix =
        ol.newSchemaDatasetFacetFieldsBuilder().name("matrix").type("array<array<long>>").build();
    OpenLineage.SchemaDatasetFacetFields buckets =
        ol.newSchemaDatasetFacetFieldsBuilder()
            .name("buckets")
            .type("map<string,array<long>>")
            .build();
    OpenLineage.SchemaDatasetFacet schema =
        ol.newSchemaDatasetFacetBuilder().fields(java.util.Arrays.asList(matrix, buckets)).build();
    OpenLineage.InputDataset ds =
        ol.newInputDatasetBuilder()
            .namespace("s3://bucket")
            .name("db.table")
            .facets(ol.newDatasetFacetsBuilder().schema(schema).build())
            .build();

    java.util.Set<String> paths =
        OpenLineageToDataHub.getSchemaMetadata(ds, config()).getFields().stream()
            .map(f -> f.getFieldPath())
            .collect(java.util.stream.Collectors.toSet());

    assertTrue(
        paths.contains("[version=2.0].[type=struct].[type=array].[type=array].[type=long].matrix"),
        "array<array<long>> should keep the inner element type: " + paths);
    assertTrue(
        paths.contains("[version=2.0].[type=struct].[type=map].[type=array].[type=long].buckets"),
        "map<string,array<long>> should keep the inner element type: " + paths);
  }

  // ---- orchestrator resolution ----

  /**
   * A RunEvent from a producer the URI heuristics do not recognise used to be rejected outright
   * with "Unable to determine orchestrator" -- an HTTP 500 -- whenever it omitted the
   * processing_engine run facet. dbt, Flink and house-built emitters commonly do omit it while
   * still sending jobType, so that facet now stands in.
   */
  @Test
  public void runEventFallsBackToJobTypeIntegrationForTheOrchestrator() throws Exception {
    OpenLineage ol = new OpenLineage(PRODUCER);
    OpenLineage.RunEvent event =
        ol.newRunEventBuilder()
            .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
            .eventTime(ZonedDateTime.now())
            .run(ol.newRunBuilder().runId(UUID.randomUUID()).build())
            .job(
                ol.newJobBuilder()
                    .namespace("ns")
                    .name("job")
                    .facets(
                        ol.newJobFacetsBuilder()
                            .jobType(
                                ol.newJobTypeJobFacetBuilder()
                                    .processingType("BATCH")
                                    .integration("DBT")
                                    .jobType("MODEL")
                                    .build())
                            .build())
                    .build())
            .inputs(Collections.emptyList())
            .outputs(Collections.emptyList())
            .build();

    DatahubJob job = OpenLineageToDataHub.convertRunEventToJob(event, configWithoutOrchestrator());

    assertEquals(job.getFlowUrn().getOrchestratorEntity(), "dbt");
    // The DataJobInfo builds its own flow URN; if the two disagreed the job would advertise a
    // DataFlow it does not belong to.
    assertEquals(
        job.getJobInfo().getFlowUrn().getOrchestratorEntity(),
        job.getFlowUrn().getOrchestratorEntity());
  }

  private static DatahubOpenlineageConfig configWithoutOrchestrator() {
    return DatahubOpenlineageConfig.builder().fabricType(FabricType.PROD).build();
  }

  // ---- eventType handling ----

  private static OpenLineage.RunEventBuilder baseEvent(OpenLineage ol) {
    return ol.newRunEventBuilder()
        .eventTime(ZonedDateTime.now())
        .run(ol.newRunBuilder().runId(UUID.randomUUID()).build())
        .job(
            ol.newJobBuilder()
                .namespace("ns")
                .name("job")
                .facets(ol.newJobFacetsBuilder().build())
                .build())
        .inputs(Collections.emptyList())
        .outputs(Collections.emptyList());
  }

  private static boolean hasAspect(List<MetadataChangeProposal> mcps, String aspectName) {
    return mcps.stream().anyMatch(m -> aspectName.equals(m.getAspectName()));
  }

  private static boolean anyAspectContains(List<MetadataChangeProposal> mcps, String needle) {
    return mcps.stream()
        .filter(m -> m.getAspect() != null)
        .anyMatch(m -> m.getAspect().getValue().asString(StandardCharsets.UTF_8).contains(needle));
  }

  @Test
  public void otherEventTypeDoesNotEmitUnknownRunResult() throws Exception {
    OpenLineage ol = new OpenLineage(PRODUCER);
    OpenLineage.RunEvent event =
        baseEvent(ol).eventType(OpenLineage.RunEvent.EventType.OTHER).build();

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertRunEventToJob(event, config()).toMcps(config());

    assertFalse(
        anyAspectContains(mcps, "$UNKNOWN"),
        "OTHER eventType must not emit a RunResultType.$UNKNOWN aspect (fails validation)");
    assertFalse(
        hasAspect(mcps, "dataProcessInstanceRunEvent"),
        "OTHER is not a run-state transition, so no dataProcessInstanceRunEvent should be emitted");
  }

  @Test
  public void missingEventTypeDoesNotThrow() throws Exception {
    OpenLineage ol = new OpenLineage(PRODUCER);
    OpenLineage.RunEvent event = baseEvent(ol).build(); // no eventType set

    DatahubJob job = OpenLineageToDataHub.convertRunEventToJob(event, config());
    List<MetadataChangeProposal> mcps = job.toMcps(config());

    assertFalse(
        anyAspectContains(mcps, "$UNKNOWN"),
        "missing eventType must not produce a $UNKNOWN aspect");
  }

  // A FAIL event whose run carries an ErrorMessageRunFacet with the given stack trace.
  private static OpenLineage.RunEvent failEventWithStackTrace(OpenLineage ol, String stackTrace) {
    OpenLineage.ErrorMessageRunFacet error =
        ol.newErrorMessageRunFacetBuilder()
            .message("boom")
            .programmingLanguage("scala")
            .stackTrace(stackTrace)
            .build();
    return ol.newRunEventBuilder()
        .eventTime(ZonedDateTime.now())
        .eventType(OpenLineage.RunEvent.EventType.FAIL)
        .run(
            ol.newRunBuilder()
                .runId(UUID.randomUUID())
                .facets(ol.newRunFacetsBuilder().errorMessage(error).build())
                .build())
        .job(
            ol.newJobBuilder()
                .namespace("ns")
                .name("job")
                .facets(ol.newJobFacetsBuilder().build())
                .build())
        .inputs(Collections.emptyList())
        .outputs(Collections.emptyList())
        .build();
  }

  @Test
  public void errorMessageRunFacetSurfacesAsCustomProperties() throws Exception {
    OpenLineage ol = new OpenLineage(PRODUCER);
    OpenLineage.RunEvent event = failEventWithStackTrace(ol, "at Foo.bar(Foo.scala:1)");

    DatahubJob job = OpenLineageToDataHub.convertRunEventToJob(event, config());
    java.util.Map<String, String> props =
        job.getDataProcessInstanceProperties().getCustomProperties();

    assertEquals(props.get("errorMessage"), "boom");
    assertEquals(props.get("programmingLanguage"), "scala");
    assertTrue(
        props.get("stackTrace") != null && props.get("stackTrace").contains("Foo.scala"),
        "stackTrace should be preserved: " + props);
  }

  @Test
  public void errorMessageStackTraceIsTruncated() throws Exception {
    OpenLineage ol = new OpenLineage(PRODUCER);
    StringBuilder hugeTrace = new StringBuilder();
    for (int i = 0; i < 20000; i++) {
      hugeTrace.append("x");
    }
    OpenLineage.RunEvent event = failEventWithStackTrace(ol, hugeTrace.toString());

    java.util.Map<String, String> props =
        OpenLineageToDataHub.convertRunEventToJob(event, config())
            .getDataProcessInstanceProperties()
            .getCustomProperties();

    String stackTrace = props.get("stackTrace");
    assertTrue(
        stackTrace.length() < 20000, "stack trace should be truncated: " + stackTrace.length());
    assertTrue(stackTrace.endsWith("...[truncated]"), "truncation marker expected");
    // small fields are kept verbatim
    assertEquals(props.get("errorMessage"), "boom");
  }

  @Test
  public void taskDocumentationJobFacetLandsOnDataJobOnly() throws Exception {
    OpenLineage ol = new OpenLineage(PRODUCER);
    // Has a ParentRunFacet -> it's a task within a flow; its doc must not leak onto the DataFlow.
    OpenLineage.ParentRunFacet parent =
        ol.newParentRunFacetBuilder()
            .job(ol.newParentRunFacetJobBuilder().namespace("ns").name("my_flow").build())
            .run(ol.newParentRunFacetRunBuilder().runId(UUID.randomUUID()).build())
            .build();
    OpenLineage.RunEvent event =
        ol.newRunEventBuilder()
            .eventTime(ZonedDateTime.now())
            .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
            .run(
                ol.newRunBuilder()
                    .runId(UUID.randomUUID())
                    .facets(ol.newRunFacetsBuilder().parent(parent).build())
                    .build())
            .job(
                ol.newJobBuilder()
                    .namespace("ns")
                    .name("my_flow.my_task")
                    .facets(
                        ol.newJobFacetsBuilder()
                            .documentation(
                                ol.newDocumentationJobFacetBuilder()
                                    .description("my task docs")
                                    .build())
                            .build())
                    .build())
            .inputs(Collections.emptyList())
            .outputs(Collections.emptyList())
            .build();

    DatahubJob job = OpenLineageToDataHub.convertRunEventToJob(event, config());
    assertEquals(job.getJobInfo().getDescription(), "my task docs");
    assertTrue(
        job.getDataFlowInfo().getDescription() == null,
        "DataFlow must not carry a task-level DocumentationJobFacet description");
  }

  @Test
  public void completeEventStillEmitsSuccessRunEvent() throws Exception {
    OpenLineage ol = new OpenLineage(PRODUCER);
    OpenLineage.RunEvent event =
        baseEvent(ol).eventType(OpenLineage.RunEvent.EventType.COMPLETE).build();

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertRunEventToJob(event, config()).toMcps(config());

    assertTrue(
        hasAspect(mcps, "dataProcessInstanceRunEvent"),
        "COMPLETE must still emit a dataProcessInstanceRunEvent (control case)");
  }
}
