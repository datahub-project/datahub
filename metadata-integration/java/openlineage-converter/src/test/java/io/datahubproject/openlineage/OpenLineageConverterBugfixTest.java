package io.datahubproject.openlineage;

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
