package io.datahubproject.openlineage;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.FabricType;
import com.linkedin.mxe.MetadataChangeProposal;
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
