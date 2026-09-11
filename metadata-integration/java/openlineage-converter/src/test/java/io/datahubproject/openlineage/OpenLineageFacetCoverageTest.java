package io.datahubproject.openlineage;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.FabricType;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.converter.OpenLineageToDataHub;
import io.datahubproject.openlineage.dataset.DatahubJob;
import io.openlineage.client.OpenLineage;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.testng.annotations.Test;

/**
 * Covers the OpenLineage facets the converter previously discarded: output statistics and lifecycle
 * state on outputs, data-quality metrics, dataset-level tags/ownership/documentation, run-level
 * nominal time / external query / extraction errors, and the job's type and source-code location.
 */
public class OpenLineageFacetCoverageTest {

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
  public void outputStatisticsAndLifecycleBecomeAnOperation() throws Exception {
    OpenLineage ol = new OpenLineage(PRODUCER);
    OpenLineage.OutputDataset output =
        ol.newOutputDatasetBuilder()
            .namespace("postgres://my-host:5432")
            .name("my_db.my_schema.events")
            .facets(
                ol.newDatasetFacetsBuilder()
                    .lifecycleStateChange(
                        ol.newLifecycleStateChangeDatasetFacetBuilder()
                            .lifecycleStateChange(
                                OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange
                                    .CREATE)
                            .build())
                    .build())
            .outputFacets(
                ol.newOutputDatasetOutputFacetsBuilder()
                    .outputStatistics(
                        ol.newOutputStatisticsOutputDatasetFacetBuilder()
                            .rowCount(1500L)
                            .size(2048L)
                            .build())
                    .build())
            .build();

    List<MetadataChangeProposal> mcps =
        mcps(ol, baseEvent(ol).outputs(Collections.singletonList(output)).build());

    assertTrue(hasAspect(mcps, "operation"), "an operation aspect should be emitted");
    String operation = aspectJson(mcps, "operation");
    assertTrue(operation.contains("\"numAffectedRows\":1500"), operation);
    assertTrue(operation.contains("CREATE"), operation);
    assertTrue(operation.contains("2048"), "size should reach custom properties: " + operation);
  }

  @Test
  public void outputStatisticsWithoutLifecycleStillRecordsAWrite() throws Exception {
    OpenLineage ol = new OpenLineage(PRODUCER);
    OpenLineage.OutputDataset output =
        ol.newOutputDatasetBuilder()
            .namespace("postgres://my-host:5432")
            .name("my_db.my_schema.events")
            .outputFacets(
                ol.newOutputDatasetOutputFacetsBuilder()
                    .outputStatistics(
                        ol.newOutputStatisticsOutputDatasetFacetBuilder().rowCount(7L).build())
                    .build())
            .build();

    List<MetadataChangeProposal> mcps =
        mcps(ol, baseEvent(ol).outputs(Collections.singletonList(output)).build());

    String operation = aspectJson(mcps, "operation");
    assertTrue(
        operation.contains("UPDATE"), "no lifecycle facet should read as UPDATE: " + operation);
  }

  @Test
  public void inputsDoNotGetAnOperation() throws Exception {
    OpenLineage ol = new OpenLineage(PRODUCER);
    OpenLineage.InputDataset input =
        ol.newInputDatasetBuilder()
            .namespace("postgres://my-host:5432")
            .name("my_db.my_schema.source")
            .build();

    List<MetadataChangeProposal> mcps =
        mcps(ol, baseEvent(ol).inputs(Collections.singletonList(input)).build());

    assertTrue(
        !hasAspect(mcps, "operation"), "a run reads inputs; only outputs record an operation");
  }

  @Test
  public void dataQualityMetricsBecomeADatasetProfile() throws Exception {
    OpenLineage ol = new OpenLineage(PRODUCER);
    OpenLineage.DataQualityMetricsDatasetFacetColumnMetricsAdditional colA =
        ol.newDataQualityMetricsDatasetFacetColumnMetricsAdditionalBuilder()
            .nullCount(3L)
            .distinctCount(97L)
            .build();
    OpenLineage.DataQualityMetricsDatasetFacetColumnMetrics columnMetrics =
        ol.newDataQualityMetricsDatasetFacetColumnMetricsBuilder().put("col_a", colA).build();
    OpenLineage.InputDataset input =
        ol.newInputDatasetBuilder()
            .namespace("postgres://my-host:5432")
            .name("my_db.my_schema.events")
            .facets(
                ol.newDatasetFacetsBuilder()
                    .dataQualityMetrics(
                        ol.newDataQualityMetricsDatasetFacetBuilder()
                            .rowCount(100L)
                            .bytes(4096L)
                            .columnMetrics(columnMetrics)
                            .build())
                    .build())
            .build();

    List<MetadataChangeProposal> mcps =
        mcps(ol, baseEvent(ol).inputs(Collections.singletonList(input)).build());

    String profile = aspectJson(mcps, "datasetProfile");
    assertTrue(profile.contains("\"rowCount\":100"), profile);
    assertTrue(profile.contains("\"sizeInBytes\":4096"), profile);
    assertTrue(profile.contains("col_a"), profile);
    assertTrue(profile.contains("\"nullCount\":3"), profile);
  }

  @Test
  public void datasetTagsAndOwnershipReachTheDataset() throws Exception {
    OpenLineage ol = new OpenLineage(PRODUCER);
    OpenLineage.InputDataset input =
        ol.newInputDatasetBuilder()
            .namespace("postgres://my-host:5432")
            .name("my_db.my_schema.events")
            .facets(
                ol.newDatasetFacetsBuilder()
                    .tags(
                        ol.newTagsDatasetFacetBuilder()
                            .tags(
                                Collections.singletonList(
                                    ol.newTagsDatasetFacetFieldsBuilder()
                                        .key("tier")
                                        .value("gold")
                                        .build()))
                            .build())
                    .ownership(
                        ol.newOwnershipDatasetFacetBuilder()
                            .owners(
                                Collections.singletonList(
                                    ol.newOwnershipDatasetFacetOwnersBuilder()
                                        .name("data_team")
                                        .type("TECHNICAL_OWNER")
                                        .build()))
                            .build())
                    .build())
            .build();

    List<MetadataChangeProposal> mcps =
        mcps(ol, baseEvent(ol).inputs(Collections.singletonList(input)).build());

    String tags = datasetAspectJson(mcps, "globalTags");
    assertTrue(tags.contains("urn:li:tag:tier:gold"), tags);
    String ownership = datasetAspectJson(mcps, "ownership");
    assertTrue(ownership.contains("urn:li:corpuser:data_team"), ownership);
    assertTrue(ownership.contains("TECHNICAL_OWNER"), ownership);
  }

  @Test
  public void descriptiveDatasetFacetsBecomeDatasetProperties() throws Exception {
    OpenLineage ol = new OpenLineage(PRODUCER);
    OpenLineage.InputDataset input =
        ol.newInputDatasetBuilder()
            .namespace("s3://my-bucket")
            .name("events")
            .facets(
                ol.newDatasetFacetsBuilder()
                    .documentation(
                        ol.newDocumentationDatasetFacetBuilder()
                            .description("Raw event landing zone")
                            .build())
                    .storage(
                        ol.newStorageDatasetFacetBuilder()
                            .storageLayer("iceberg")
                            .fileFormat("parquet")
                            .build())
                    .datasetType(
                        ol.newDatasetTypeDatasetFacetBuilder().datasetType("TABLE").build())
                    .version(ol.newDatasetVersionDatasetFacetBuilder().datasetVersion("42").build())
                    .build())
            .build();

    List<MetadataChangeProposal> mcps =
        mcps(ol, baseEvent(ol).inputs(Collections.singletonList(input)).build());

    String properties = datasetAspectJson(mcps, "datasetProperties");
    assertTrue(properties.contains("Raw event landing zone"), properties);
    assertTrue(properties.contains("iceberg"), properties);
    assertTrue(properties.contains("parquet"), properties);
    assertTrue(properties.contains("TABLE"), properties);
    assertTrue(properties.contains("42"), properties);
  }

  @Test
  public void runDiagnosticsReachDataProcessInstanceProperties() throws Exception {
    OpenLineage ol = new OpenLineage(PRODUCER);
    ZonedDateTime nominal = ZonedDateTime.parse("2024-03-01T00:00:00Z");
    OpenLineage.RunFacets runFacets =
        ol.newRunFacetsBuilder()
            .nominalTime(
                ol.newNominalTimeRunFacetBuilder()
                    .nominalStartTime(nominal)
                    .nominalEndTime(nominal.plusHours(1))
                    .build())
            .externalQuery(
                ol.newExternalQueryRunFacetBuilder()
                    .externalQueryId("job_abc123")
                    .source("bigquery")
                    .build())
            .extractionError(
                ol.newExtractionErrorRunFacetBuilder()
                    .totalTasks(10L)
                    .failedTasks(2L)
                    .errors(
                        Collections.singletonList(
                            ol.newExtractionErrorRunFacetErrorsBuilder()
                                .errorMessage("could not parse statement")
                                .task("task_7")
                                .build()))
                    .build())
            .build();

    OpenLineage.RunEvent event =
        baseEvent(ol)
            .run(ol.newRunBuilder().runId(UUID.randomUUID()).facets(runFacets).build())
            .build();

    String dpiProperties = aspectJson(mcps(ol, event), "dataProcessInstanceProperties");
    assertTrue(dpiProperties.contains("2024-03-01T00:00"), dpiProperties);
    assertTrue(dpiProperties.contains("job_abc123"), dpiProperties);
    assertTrue(dpiProperties.contains("bigquery"), dpiProperties);
    assertTrue(dpiProperties.contains("could not parse statement"), dpiProperties);
    assertTrue(dpiProperties.contains("extractionErrorFailedTasks"), dpiProperties);
  }

  @Test
  public void jobTypeAndSourceCodeLocationReachTheDataJob() throws Exception {
    OpenLineage ol = new OpenLineage(PRODUCER);
    OpenLineage.JobFacets jobFacets =
        ol.newJobFacetsBuilder()
            .jobType(
                ol.newJobTypeJobFacetBuilder()
                    .processingType("STREAMING")
                    .integration("SPARK")
                    .jobType("QUERY")
                    .build())
            .sourceCodeLocation(
                ol.newSourceCodeLocationJobFacetBuilder()
                    .type("git")
                    .url(URI.create("https://github.com/my-org/my-repo"))
                    .branch("main")
                    .build())
            .build();

    OpenLineage.RunEvent event =
        baseEvent(ol)
            .job(ol.newJobBuilder().namespace("ns").name("job").facets(jobFacets).build())
            .build();

    List<MetadataChangeProposal> mcps = mcps(ol, event);

    String jobInfo = aspectJson(mcps, "dataJobInfo");
    assertTrue(jobInfo.contains("STREAMING"), jobInfo);
    assertTrue(jobInfo.contains("SPARK"), jobInfo);

    String memory = aspectJson(mcps, "institutionalMemory");
    assertTrue(memory.contains("https://github.com/my-org/my-repo"), memory);
    assertTrue(memory.contains("main"), memory);
  }

  @Test
  public void eventsWithoutTheNewFacetsAreUnchanged() throws Exception {
    OpenLineage ol = new OpenLineage(PRODUCER);
    OpenLineage.InputDataset input =
        ol.newInputDatasetBuilder()
            .namespace("postgres://my-host:5432")
            .name("my_db.my_schema.events")
            .build();

    List<MetadataChangeProposal> mcps =
        mcps(ol, baseEvent(ol).inputs(Collections.singletonList(input)).build());

    // Additive change: an event carrying none of the new facets emits none of the new aspects.
    for (String aspect :
        new String[] {"operation", "datasetProfile", "institutionalMemory", "datasetProperties"}) {
      assertTrue(!hasAspect(mcps, aspect), aspect + " should not be emitted without its facet");
    }
    assertNotNull(aspectJson(mcps, "dataJobInfo"));
  }

  @Test
  public void datasetTagsAndOwnershipUsePatchSoOtherSourcesSurvive() throws Exception {
    // A full upsert of globalTags/ownership/datasetProperties would erase whatever another
    // connector had already written to the same dataset. In patch mode these must be patches.
    OpenLineage ol = new OpenLineage(PRODUCER);
    OpenLineage.InputDataset input =
        ol.newInputDatasetBuilder()
            .namespace("postgres://my-host:5432")
            .name("my_db.my_schema.events")
            .facets(
                ol.newDatasetFacetsBuilder()
                    .tags(
                        ol.newTagsDatasetFacetBuilder()
                            .tags(
                                Collections.singletonList(
                                    ol.newTagsDatasetFacetFieldsBuilder()
                                        .key("tier")
                                        .value("gold")
                                        .build()))
                            .build())
                    .ownership(
                        ol.newOwnershipDatasetFacetBuilder()
                            .owners(
                                Collections.singletonList(
                                    ol.newOwnershipDatasetFacetOwnersBuilder()
                                        .name("data_team")
                                        .build()))
                            .build())
                    .documentation(
                        ol.newDocumentationDatasetFacetBuilder().description("desc").build())
                    .build())
            .build();

    DatahubOpenlineageConfig patching =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.PROD)
            .orchestrator("airflow")
            .materializeDataset(true)
            .includeSchemaMetadata(true)
            .usePatch(true)
            .build();
    DatahubJob job =
        OpenLineageToDataHub.convertRunEventToJob(
            baseEvent(ol).inputs(Collections.singletonList(input)).build(), patching);

    for (String aspect : new String[] {"globalTags", "ownership", "datasetProperties"}) {
      MetadataChangeProposal mcp =
          job.toMcps(patching).stream()
              .filter(m -> aspect.equals(m.getAspectName()) && "dataset".equals(m.getEntityType()))
              .findFirst()
              .orElseThrow(() -> new AssertionError("no dataset " + aspect + " emitted"));
      assertTrue(
          "PATCH".equals(String.valueOf(mcp.getChangeType())),
          aspect
              + " must be patched, not overwritten, when usePatch is on; got "
              + mcp.getChangeType());
    }
  }

  @Test
  public void lifecycleDetailSurvivesTheOperationTypeCollapse() throws Exception {
    // TRUNCATE has no DataHub equivalent and maps to DELETE; the original word must not be lost.
    OpenLineage ol = new OpenLineage(PRODUCER);
    OpenLineage.OutputDataset output =
        ol.newOutputDatasetBuilder()
            .namespace("postgres://my-host:5432")
            .name("my_db.my_schema.events")
            .facets(
                ol.newDatasetFacetsBuilder()
                    .lifecycleStateChange(
                        ol.newLifecycleStateChangeDatasetFacetBuilder()
                            .lifecycleStateChange(
                                OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange
                                    .TRUNCATE)
                            .build())
                    .build())
            .build();

    String operation =
        aspectJson(
            mcps(ol, baseEvent(ol).outputs(Collections.singletonList(output)).build()),
            "operation");
    assertTrue(operation.contains("DELETE"), operation);
    assertTrue(
        operation.contains("TRUNCATE"), "original lifecycle word should survive: " + operation);
  }

  // ---- helpers ----

  private static OpenLineage.RunEventBuilder baseEvent(OpenLineage ol) {
    return ol.newRunEventBuilder()
        .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
        .eventTime(ZonedDateTime.parse("2024-03-01T10:00:00Z"))
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

  private static List<MetadataChangeProposal> mcps(OpenLineage ol, OpenLineage.RunEvent event)
      throws IOException, URISyntaxException {
    DatahubOpenlineageConfig config = config();
    DatahubJob job = OpenLineageToDataHub.convertRunEventToJob(event, config);
    return job.toMcps(config);
  }

  private static boolean hasAspect(List<MetadataChangeProposal> mcps, String aspectName) {
    return mcps.stream().anyMatch(m -> aspectName.equals(m.getAspectName()));
  }

  private static String aspectJson(List<MetadataChangeProposal> mcps, String aspectName) {
    return mcps.stream()
        .filter(m -> aspectName.equals(m.getAspectName()) && m.getAspect() != null)
        .map(m -> m.getAspect().getValue().asString(StandardCharsets.UTF_8))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no " + aspectName + " aspect emitted"));
  }

  /**
   * Same as {@link #aspectJson} but restricted to dataset entities, since job and dataset share
   * aspect names such as {@code globalTags} and {@code ownership}.
   */
  private static String datasetAspectJson(List<MetadataChangeProposal> mcps, String aspectName) {
    return mcps.stream()
        .filter(
            m ->
                aspectName.equals(m.getAspectName())
                    && m.getAspect() != null
                    && "dataset".equals(m.getEntityType()))
        .map(m -> m.getAspect().getValue().asString(StandardCharsets.UTF_8))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no dataset " + aspectName + " aspect emitted"));
  }
}
