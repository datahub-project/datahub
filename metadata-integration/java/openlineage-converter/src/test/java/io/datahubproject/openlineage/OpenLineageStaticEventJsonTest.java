package io.datahubproject.openlineage;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.linkedin.common.FabricType;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.converter.OpenLineageToDataHub;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Scanner;
import org.testng.annotations.Test;

/**
 * {@link OpenLineageStaticEventTest} builds JobEvent and DatasetEvent objects with the client's
 * builders, which never exercises deserialization. These start from the JSON a producer sends,
 * which is the path the REST endpoint actually takes.
 */
public class OpenLineageStaticEventJsonTest {

  private static String load(String resource) {
    try (InputStream in =
        OpenLineageStaticEventJsonTest.class.getClassLoader().getResourceAsStream(resource)) {
      assertNotNull(in, "missing test resource " + resource);
      return new Scanner(in, StandardCharsets.UTF_8.name()).useDelimiter("\\A").next();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static DatahubOpenlineageConfig config() {
    return DatahubOpenlineageConfig.builder()
        .fabricType(FabricType.PROD)
        .orchestrator("airflow")
        .materializeDataset(true)
        .includeSchemaMetadata(true)
        .build();
  }

  private static boolean hasAspect(List<MetadataChangeProposal> mcps, String aspectName) {
    return mcps.stream().anyMatch(m -> aspectName.equals(m.getAspectName()));
  }

  @Test
  public void jobEventJsonProducesJobAndLineageButNoRun() throws Exception {
    OpenLineage.JobEvent event =
        OpenLineageClientUtils.fromJson(
            load("job_event.json"), new TypeReference<OpenLineage.JobEvent>() {});
    assertNotNull(event.getJob(), "job must survive deserialization");
    assertNotNull(event.getJob().getFacets().getJobType(), "jobType facet must survive");
    assertNotNull(
        event.getJob().getFacets().getSourceCodeLocation(), "sourceCodeLocation must survive");

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertJobEventToJob(event, config()).toMcps(config());

    assertTrue(hasAspect(mcps, "dataJobInfo"));
    assertTrue(hasAspect(mcps, "dataJobInputOutput"));
    assertTrue(hasAspect(mcps, "institutionalMemory"), "sourceCodeLocation should become a link");
    assertFalse(
        hasAspect(mcps, "dataProcessInstanceProperties"),
        "a JobEvent carries no run, so no DataProcessInstance");
  }

  @Test
  public void datasetEventJsonProducesDatasetAspectsOnly() throws Exception {
    OpenLineage.DatasetEvent event =
        OpenLineageClientUtils.fromJson(
            load("dataset_event.json"), new TypeReference<OpenLineage.DatasetEvent>() {});
    assertNotNull(event.getDataset(), "dataset must survive deserialization");

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertDatasetEventToMcps(event, config());

    assertTrue(hasAspect(mcps, "datasetProperties"), "documentation should become properties");
    assertTrue(
        mcps.stream().allMatch(m -> "dataset".equals(m.getEntityType())),
        "a DatasetEvent touches only the dataset entity");
  }

  /**
   * Producers do omit {@code schemaURL}. The endpoint falls back to the event's shape, so a
   * JobEvent without it must still deserialize as a JobEvent rather than a RunEvent with a null
   * run.
   */
  @Test
  public void jobEventWithoutSchemaUrlStillDeserializes() throws Exception {
    OpenLineage.JobEvent event =
        OpenLineageClientUtils.fromJson(
            load("job_event_no_schema_url.json"), new TypeReference<OpenLineage.JobEvent>() {});
    assertNotNull(event.getJob());
    assertTrue(
        hasAspect(
            OpenLineageToDataHub.convertJobEventToJob(event, config()).toMcps(config()),
            "dataJobInfo"));
  }
}
