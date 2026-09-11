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
import org.testng.annotations.Test;

/**
 * OpenLineage 2.0's static-lineage events: {@code JobEvent} and {@code DatasetEvent} carry metadata
 * with no run attached, which is how a producer describes a job or a table it did not just execute
 * or write. Neither should produce a DataProcessInstance.
 */
public class OpenLineageStaticEventTest {

  private static final URI PRODUCER = URI.create("https://github.com/apache/airflow/tree/1.0.0");

  private static DatahubOpenlineageConfig config() {
    return DatahubOpenlineageConfig.builder()
        .fabricType(FabricType.PROD)
        .orchestrator("airflow")
        .materializeDataset(true)
        .includeSchemaMetadata(true)
        .build();
  }

  @Test
  public void jobEventProducesJobAndLineageButNoRun() throws Exception {
    OpenLineage ol = new OpenLineage(PRODUCER);
    OpenLineage.JobEvent event =
        ol.newJobEventBuilder()
            .eventTime(ZonedDateTime.parse("2024-03-01T10:00:00Z"))
            .job(
                ol.newJobBuilder()
                    .namespace("my_namespace")
                    .name("my_job")
                    .facets(
                        ol.newJobFacetsBuilder()
                            .documentation(
                                ol.newDocumentationJobFacetBuilder()
                                    .description("Static job description")
                                    .build())
                            .build())
                    .build())
            .inputs(
                Collections.singletonList(
                    ol.newInputDatasetBuilder()
                        .namespace("postgres://my-host:5432")
                        .name("my_db.my_schema.source")
                        .build()))
            .outputs(
                Collections.singletonList(
                    ol.newOutputDatasetBuilder()
                        .namespace("postgres://my-host:5432")
                        .name("my_db.my_schema.target")
                        .build()))
            .build();

    DatahubJob job = OpenLineageToDataHub.convertJobEventToJob(event, config());
    List<MetadataChangeProposal> mcps = job.toMcps(config());

    assertTrue(hasAspect(mcps, "dataJobInfo"), "a DataJob should be created");
    assertTrue(hasAspect(mcps, "dataJobInputOutput"), "lineage edges should be emitted");
    assertTrue(contains(mcps, "Static job description"), "job description should survive");
    assertTrue(contains(mcps, "my_db.my_schema.source"), "input edge should be present");
    assertTrue(contains(mcps, "my_db.my_schema.target"), "output edge should be present");

    // There is no run, so nothing should describe one.
    assertFalse(
        hasAspect(mcps, "dataProcessInstanceProperties"),
        "a JobEvent carries no run, so no DataProcessInstance should be produced");
    assertFalse(hasAspect(mcps, "dataProcessInstanceRunEvent"), "no run event without a run");
  }

  @Test
  public void datasetEventProducesDatasetAspectsOnly() throws Exception {
    OpenLineage ol = new OpenLineage(PRODUCER);
    OpenLineage.DatasetEvent event =
        ol.newDatasetEventBuilder()
            .eventTime(ZonedDateTime.parse("2024-03-01T10:00:00Z"))
            .dataset(
                ol.newStaticDatasetBuilder()
                    .namespace("postgres://my-host:5432")
                    .name("my_db.my_schema.events")
                    .facets(
                        ol.newDatasetFacetsBuilder()
                            .schema(
                                ol.newSchemaDatasetFacetBuilder()
                                    .fields(
                                        Collections.singletonList(
                                            ol.newSchemaDatasetFacetFieldsBuilder()
                                                .name("col_a")
                                                .type("string")
                                                .build()))
                                    .build())
                            .documentation(
                                ol.newDocumentationDatasetFacetBuilder()
                                    .description("Catalogued without a run")
                                    .build())
                            .build())
                    .build())
            .build();

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertDatasetEventToMcps(event, config());

    assertTrue(hasAspect(mcps, "schemaMetadata"), "schema should be emitted");
    assertTrue(hasAspect(mcps, "datasetProperties"), "documentation should become properties");
    assertTrue(contains(mcps, "col_a"), "schema field should survive");
    assertTrue(contains(mcps, "Catalogued without a run"));

    // No job and no run means no job-shaped aspects at all.
    for (String aspect :
        new String[] {
          "dataJobInfo", "dataFlowInfo", "dataJobInputOutput", "dataProcessInstanceProperties"
        }) {
      assertFalse(hasAspect(mcps, aspect), aspect + " must not be emitted for a DatasetEvent");
    }
    assertTrue(
        mcps.stream().allMatch(m -> "dataset".equals(m.getEntityType())),
        "every proposal should target the dataset entity");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void datasetEventWithoutADatasetIsRejected() throws Exception {
    OpenLineage ol = new OpenLineage(PRODUCER);
    OpenLineage.DatasetEvent event =
        ol.newDatasetEventBuilder().eventTime(ZonedDateTime.now()).build();
    OpenLineageToDataHub.convertDatasetEventToMcps(event, config());
  }

  private static boolean hasAspect(List<MetadataChangeProposal> mcps, String aspectName) {
    return mcps.stream().anyMatch(m -> aspectName.equals(m.getAspectName()));
  }

  private static boolean contains(List<MetadataChangeProposal> mcps, String needle) {
    return mcps.stream()
        .anyMatch(
            m ->
                (m.getAspect() != null
                        && m.getAspect()
                            .getValue()
                            .asString(StandardCharsets.UTF_8)
                            .contains(needle))
                    || (m.getEntityUrn() != null && m.getEntityUrn().toString().contains(needle)));
  }
}
