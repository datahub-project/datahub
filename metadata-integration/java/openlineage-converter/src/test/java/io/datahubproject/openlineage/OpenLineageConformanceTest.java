package io.datahubproject.openlineage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.common.FabricType;
import com.linkedin.common.Operation;
import com.linkedin.container.Container;
import com.linkedin.container.ContainerProperties;
import com.linkedin.data.DataMap;
import com.linkedin.data.codec.JacksonDataCodec;
import com.linkedin.data.schema.validation.ValidateDataAgainstSchema;
import com.linkedin.data.schema.validation.ValidationOptions;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.dataprocess.DataProcessInstanceOutput;
import com.linkedin.dataprocess.DataProcessInstanceProperties;
import com.linkedin.dataprocess.DataProcessInstanceRunEvent;
import com.linkedin.dataset.DatasetProfile;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.converter.OpenLineageToDataHub;
import io.openlineage.client.OpenLineage;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class OpenLineageConformanceTest {
  private static final URI CUSTOM_PRODUCER = URI.create("https://example.com/my-producer");
  private static final ZonedDateTime EVENT_TIME = ZonedDateTime.parse("2026-04-14T10:00:00Z");

  private static DatahubOpenlineageConfig config() {
    return DatahubOpenlineageConfig.builder()
        .fabricType(FabricType.PROD)
        .materializeDataset(true)
        .includeSchemaMetadata(true)
        .usePatch(false)
        .build();
  }

  private static OpenLineage.RunBuilder runBuilder(OpenLineage openLineage) {
    return openLineage.newRunBuilder().runId(UUID.randomUUID());
  }

  private static OpenLineage.JobBuilder jobBuilder(OpenLineage openLineage) {
    return openLineage.newJobBuilder().namespace("crm").name("load.customer");
  }

  private static OpenLineage.RunEventBuilder runEventBuilder(OpenLineage openLineage) {
    return openLineage
        .newRunEventBuilder()
        .eventTime(EVENT_TIME)
        .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
        .run(runBuilder(openLineage).build())
        .job(jobBuilder(openLineage).build())
        .inputs(Collections.emptyList())
        .outputs(Collections.emptyList());
  }

  private static OpenLineage.JobEventBuilder jobEventBuilder(OpenLineage openLineage) {
    return openLineage
        .newJobEventBuilder()
        .eventTime(EVENT_TIME)
        .job(jobBuilder(openLineage).build())
        .inputs(Collections.emptyList())
        .outputs(Collections.emptyList());
  }

  private static OpenLineage.DatasetEventBuilder datasetEventBuilder(OpenLineage openLineage) {
    return openLineage
        .newDatasetEventBuilder()
        .eventTime(EVENT_TIME)
        .dataset(openLineage.newStaticDatasetBuilder().namespace("crm").name("customer").build());
  }

  @Test
  public void testArbitraryProducerPathDoesNotDeriveOrchestrator() throws Exception {
    OpenLineage openLineage = new OpenLineage(CUSTOM_PRODUCER);
    OpenLineage.RunEvent event = runEventBuilder(openLineage).build();

    assertEquals(
        OpenLineageToDataHub.convertRunEventToJob(
                event,
                DatahubOpenlineageConfig.builder()
                    .fabricType(FabricType.PROD)
                    .materializeDataset(true)
                    .includeSchemaMetadata(true)
                    .build())
            .getFlowUrn()
            .getOrchestratorEntity(),
        "unknown");
  }

  @Test
  public void testConfiguredOrchestratorOverridesTypedFacets() throws Exception {
    OpenLineage openLineage =
        new OpenLineage(
            URI.create("https://github.com/OpenLineage/OpenLineage/tree/1.45.0/integration/spark"));
    OpenLineage.RunEvent event =
        runEventBuilder(openLineage)
            .run(
                runBuilder(openLineage)
                    .facets(
                        openLineage
                            .newRunFacetsBuilder()
                            .processing_engine(
                                openLineage.newProcessingEngineRunFacet("1", "spark", "1.45.0"))
                            .build())
                    .build())
            .job(
                jobBuilder(openLineage)
                    .facets(
                        openLineage
                            .newJobFacetsBuilder()
                            .jobType(openLineage.newJobTypeJobFacet("BATCH", "airflow", "TASK"))
                            .build())
                    .build())
            .build();

    assertEquals(
        OpenLineageToDataHub.convertRunEventToJob(
                event,
                DatahubOpenlineageConfig.builder()
                    .fabricType(FabricType.PROD)
                    .materializeDataset(true)
                    .includeSchemaMetadata(true)
                    .orchestrator("dagster")
                    .build())
            .getFlowUrn()
            .getOrchestratorEntity(),
        "dagster");
  }

  @Test
  public void testKnownProducerUriMapsToOrchestrator() throws Exception {
    OpenLineage openLineage =
        new OpenLineage(
            URI.create(
                "https://github.com/OpenLineage/OpenLineage/tree/0.30.0/integration/airflow"));
    OpenLineage.RunEvent event = runEventBuilder(openLineage).build();

    assertEquals(
        OpenLineageToDataHub.convertRunEventToJob(
                event,
                DatahubOpenlineageConfig.builder()
                    .fabricType(FabricType.PROD)
                    .materializeDataset(true)
                    .includeSchemaMetadata(true)
                    .build())
            .getFlowUrn()
            .getOrchestratorEntity(),
        "airflow");
  }

  @Test
  public void testDottedJobNamePreservesFullDataJobId() throws Exception {
    OpenLineage openLineage = new OpenLineage(CUSTOM_PRODUCER);
    OpenLineage.RunEvent dotted =
        runEventBuilder(openLineage)
            .job(openLineage.newJobBuilder().namespace("crm").name("flow.group.task").build())
            .build();
    OpenLineage.RunEvent single =
        runEventBuilder(openLineage)
            .job(openLineage.newJobBuilder().namespace("crm").name("singleTask").build())
            .build();

    assertEquals(
        OpenLineageToDataHub.convertRunEventToJob(dotted, config()).getJobUrn().toString(),
        "urn:li:dataJob:(urn:li:dataFlow:(unknown,flow,crm),flow.group.task)");
    assertEquals(
        OpenLineageToDataHub.convertRunEventToJob(single, config()).getJobUrn().toString(),
        "urn:li:dataJob:(urn:li:dataFlow:(unknown,singleTask,crm),singleTask)");
  }

  @Test
  public void testOtherEventTypeDoesNotEmitInvalidRunResult() throws Exception {
    OpenLineage openLineage = new OpenLineage(CUSTOM_PRODUCER);
    OpenLineage.RunEvent event =
        runEventBuilder(openLineage).eventType(OpenLineage.RunEvent.EventType.OTHER).build();

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertRunEventToJob(event, config()).toMcps(config());

    assertFalse(hasAspect(mcps, "dataProcessInstanceRunEvent"));
  }

  @Test
  public void testRunFacetsPopulateDataProcessInstanceMetadata() throws Exception {
    OpenLineage openLineage = new OpenLineage(CUSTOM_PRODUCER);
    UUID parentRunId = UUID.randomUUID();
    ZonedDateTime nominalStart = ZonedDateTime.parse("2026-04-14T09:00:00Z");
    ZonedDateTime nominalEnd = ZonedDateTime.parse("2026-04-14T10:00:00Z");
    OpenLineage.RunFacets runFacets =
        openLineage
            .newRunFacetsBuilder()
            .processing_engine(openLineage.newProcessingEngineRunFacet("1", "spark", "1.45.0"))
            .nominalTime(openLineage.newNominalTimeRunFacet(nominalStart, nominalEnd))
            .errorMessage(
                openLineage.newErrorMessageRunFacet(
                    "boom", "java", "java.lang.RuntimeException: boom"))
            .environmentVariables(
                openLineage.newEnvironmentVariablesRunFacet(
                    List.of(
                        openLineage.newEnvironmentVariable("TEAM", "metadata"),
                        openLineage.newEnvironmentVariable("API_TOKEN", "secret-token"))))
            .parent(
                openLineage
                    .newParentRunFacetBuilder()
                    .run(openLineage.newParentRunFacetRunBuilder().runId(parentRunId).build())
                    .job(openLineage.newParentRunFacetJob("crm", "parent.flow"))
                    .build())
            .build();
    OpenLineage.RunEvent event =
        runEventBuilder(openLineage)
            .eventTime(ZonedDateTime.parse("2026-04-14T10:01:00Z"))
            .eventType(OpenLineage.RunEvent.EventType.FAIL)
            .run(runBuilder(openLineage).facets(runFacets).build())
            .build();

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertRunEventToJob(event, config()).toMcps(config());

    assertAspectsValid(mcps, "dataProcessInstanceProperties", DataProcessInstanceProperties::new);
    assertAspectsValid(mcps, "dataProcessInstanceRunEvent", DataProcessInstanceRunEvent::new);
    String properties = aspectJson(mcps, "dataProcessInstanceProperties");
    assertTrue(properties.contains("nominalStartTime"));
    assertTrue(properties.contains("nominalEndTime"));
    assertTrue(properties.contains("errorMessage"));
    assertTrue(properties.contains("boom"));
    assertTrue(properties.contains("env.TEAM"));
    assertTrue(properties.contains("metadata"));
    assertTrue(properties.contains("env.API_TOKEN"));
    assertTrue(properties.contains("[REDACTED]"));
    assertFalse(properties.contains("secret-token"));

    String relationships = aspectJson(mcps, "dataProcessInstanceRelationships");
    assertTrue(relationships.contains("urn:li:dataProcessInstance:" + parentRunId));

    String inputOutput = aspectJson(mcps, "dataJobInputOutput");
    assertTrue(
        inputOutput.contains("urn:li:dataJob:(urn:li:dataFlow:(spark,parent,crm),parent.flow)"));
  }

  @Test
  public void testSensitiveEnvironmentVariablesAreRedactedIndependentOfDefaultLocale()
      throws Exception {
    Locale original = Locale.getDefault();
    try {
      Locale.setDefault(Locale.forLanguageTag("tr"));
      OpenLineage openLineage = new OpenLineage(CUSTOM_PRODUCER);
      OpenLineage.RunEvent event =
          runEventBuilder(openLineage)
              .eventTime(ZonedDateTime.parse("2026-04-14T10:01:00Z"))
              .run(
                  runBuilder(openLineage)
                      .facets(
                          openLineage
                              .newRunFacetsBuilder()
                              .environmentVariables(
                                  openLineage.newEnvironmentVariablesRunFacet(
                                      List.of(
                                          openLineage.newEnvironmentVariable(
                                              "credential", "secret-value"))))
                              .build())
                      .build())
              .build();

      String properties =
          aspectJson(
              OpenLineageToDataHub.convertRunEventToJob(event, config()).toMcps(config()),
              "dataProcessInstanceProperties");

      assertTrue(properties.contains("[REDACTED]"));
      assertFalse(properties.contains("secret-value"));
    } finally {
      Locale.setDefault(original);
    }
  }

  @Test
  public void testJobEventTargetsDataJobWithoutProcessInstance() throws Exception {
    OpenLineage openLineage = new OpenLineage(CUSTOM_PRODUCER);
    OpenLineage.JobEvent event =
        jobEventBuilder(openLineage)
            .job(
                jobBuilder(openLineage)
                    .facets(
                        openLineage
                            .newJobFacetsBuilder()
                            .documentation(
                                openLineage.newDocumentationJobFacet("Load customer docs", null))
                            .ownership(
                                openLineage
                                    .newOwnershipJobFacetBuilder()
                                    .owners(
                                        List.of(
                                            openLineage.newOwnershipJobFacetOwners(
                                                "alice", "TECHNICAL_OWNER")))
                                    .build())
                            .build())
                    .build())
            .build();

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertJobEventToJob(event, config()).toMcps(config());

    assertTrue(hasAspect(mcps, "dataJobInputOutput"));
    assertEquals(countAspects(mcps, "dataJob", "ownership"), 1L);
    assertEquals(countAspects(mcps, "dataFlow", "ownership"), 0L);
    assertTrue(hasAspectContaining(mcps, "dataJob", "dataJobInfo", "Load customer docs"));
    assertFalse(hasAspectContaining(mcps, "dataFlow", "dataFlowInfo", "Load customer docs"));
    assertFalse(mcps.stream().anyMatch(mcp -> "dataProcessInstance".equals(mcp.getEntityType())));
  }

  @Test
  public void testJobEventAssertionsDoNotEmitRunHistory() throws Exception {
    OpenLineage openLineage = new OpenLineage(CUSTOM_PRODUCER);
    OpenLineage.InputDataset input =
        openLineage
            .newInputDatasetBuilder()
            .namespace("snowflake")
            .name("db.schema.customer")
            .inputFacets(
                openLineage
                    .newInputDatasetInputFacetsBuilder()
                    .dataQualityAssertions(
                        openLineage.newDataQualityAssertionsDatasetFacet(
                            List.of(
                                openLineage
                                    .newDataQualityAssertionsDatasetFacetAssertionsBuilder()
                                    .assertion("email is not null")
                                    .success(true)
                                    .column("email")
                                    .build())))
                    .build())
            .build();
    OpenLineage.JobEvent event =
        jobEventBuilder(openLineage)
            .eventTime(ZonedDateTime.parse("2026-04-14T10:01:00Z"))
            .inputs(List.of(input))
            .build();

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertJobEventToJob(event, config()).toMcps(config());

    assertEquals(countAspects(mcps, "assertion", "assertionInfo"), 1);
    assertEquals(countAspects(mcps, "assertion", "assertionRunEvent"), 0);
    assertFalse(mcps.stream().anyMatch(mcp -> "dataProcessInstance".equals(mcp.getEntityType())));
  }

  @Test
  public void testDatasetEventMaterializesDatasetAspects() throws Exception {
    OpenLineage openLineage = new OpenLineage(CUSTOM_PRODUCER);
    OpenLineage.DatasetEvent event =
        datasetEventBuilder(openLineage)
            .dataset(
                openLineage
                    .newStaticDatasetBuilder()
                    .namespace("snowflake")
                    .name("db.schema.table")
                    .build())
            .build();

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertDatasetEventToMcps(event, config());

    assertTrue(hasAspect(mcps, "datasetKey"));
    assertTrue(hasAspect(mcps, "status"));
  }

  @Test
  public void testNestedSchemaFieldsAreFlattenedWithStablePaths() throws Exception {
    OpenLineage openLineage = new OpenLineage(CUSTOM_PRODUCER);
    OpenLineage.SchemaDatasetFacetFields leaf =
        new OpenLineage.SchemaDatasetFacetFieldsBuilder()
            .name("amount")
            .type("DECIMAL")
            .description("Order amount")
            .build();
    OpenLineage.SchemaDatasetFacetFields parent =
        new OpenLineage.SchemaDatasetFacetFieldsBuilder()
            .name("order")
            .type("STRUCT")
            .fields(List.of(leaf))
            .build();
    OpenLineage.DatasetEvent event =
        datasetEventBuilder(openLineage)
            .dataset(
                openLineage
                    .newStaticDatasetBuilder()
                    .namespace("snowflake")
                    .name("db.schema.orders")
                    .facets(
                        openLineage
                            .newDatasetFacetsBuilder()
                            .schema(
                                openLineage
                                    .newSchemaDatasetFacetBuilder()
                                    .fields(List.of(parent))
                                    .build())
                            .build())
                    .build())
            .build();

    String schema =
        aspectJson(
            OpenLineageToDataHub.convertDatasetEventToMcps(event, config()), "schemaMetadata");

    assertTrue(schema.contains("order.amount"));
    assertTrue(schema.contains("Order amount"));
  }

  @Test
  public void testDatasetEventAlwaysEmitsAnchorWhenMaterializationIsDisabled() throws Exception {
    OpenLineage openLineage = new OpenLineage(CUSTOM_PRODUCER);
    OpenLineage.DatasetEvent event =
        datasetEventBuilder(openLineage)
            .dataset(
                openLineage
                    .newStaticDatasetBuilder()
                    .namespace("snowflake")
                    .name("db.schema.table")
                    .build())
            .build();
    DatahubOpenlineageConfig noMaterialization =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.PROD)
            .materializeDataset(false)
            .includeSchemaMetadata(false)
            .build();

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertDatasetEventToMcps(event, noMaterialization);

    assertTrue(hasAspect(mcps, "datasetKey"));
    assertTrue(hasAspect(mcps, "status"));
  }

  @Test
  public void testDatasetEventIdentityDoesNotIncludeProducer() throws Exception {
    OpenLineage firstProducer = new OpenLineage(URI.create("https://one.example.com/client"));
    OpenLineage secondProducer = new OpenLineage(URI.create("https://two.example.com/client"));
    OpenLineage.StaticDataset dataset =
        firstProducer
            .newStaticDatasetBuilder()
            .namespace("snowflake")
            .name("db.schema.table")
            .build();
    OpenLineage.DatasetEvent firstEvent =
        datasetEventBuilder(firstProducer).dataset(dataset).build();
    OpenLineage.DatasetEvent secondEvent =
        datasetEventBuilder(secondProducer).dataset(dataset).build();

    String firstUrn =
        OpenLineageToDataHub.convertDatasetEventToMcps(firstEvent, config())
            .get(0)
            .getEntityUrn()
            .toString();
    String secondUrn =
        OpenLineageToDataHub.convertDatasetEventToMcps(secondEvent, config())
            .get(0)
            .getEntityUrn()
            .toString();

    assertEquals(firstUrn, secondUrn);
  }

  @Test
  public void testTruncateLifecycleDoesNotRemoveDataset() throws Exception {
    OpenLineage openLineage = new OpenLineage(CUSTOM_PRODUCER);
    OpenLineage.StaticDataset dataset =
        openLineage
            .newStaticDatasetBuilder()
            .namespace("snowflake")
            .name("db.schema.customer")
            .facets(
                openLineage
                    .newDatasetFacetsBuilder()
                    .lifecycleStateChange(
                        openLineage.newLifecycleStateChangeDatasetFacet(
                            OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange
                                .TRUNCATE,
                            null))
                    .build())
            .build();
    OpenLineage.DatasetEvent event =
        datasetEventBuilder(openLineage)
            .eventTime(ZonedDateTime.parse("2026-04-14T10:01:00Z"))
            .dataset(dataset)
            .build();

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertDatasetEventToMcps(event, config());

    assertTrue(hasAspectContaining(mcps, "status", "\"removed\":false"));
    assertFalse(hasAspectContaining(mcps, "status", "\"removed\":true"));
  }

  @Test
  public void testDatasetFacetsAndOutputStatisticsEmitDatasetAspects() throws Exception {
    OpenLineage openLineage = new OpenLineage(CUSTOM_PRODUCER);
    OpenLineage.DatasetFacets datasetFacets =
        openLineage
            .newDatasetFacetsBuilder()
            .documentation(openLineage.newDocumentationDatasetFacet("Customer output", null))
            .dataSource(
                openLineage.newDatasourceDatasetFacet(
                    "warehouse", URI.create("https://warehouse.example.com/db/schema/customer")))
            .ownership(
                openLineage.newOwnershipDatasetFacet(
                    List.of(openLineage.newOwnershipDatasetFacetOwners("alice", "DATAOWNER"))))
            .tags(
                openLineage.newTagsDatasetFacet(
                    List.of(openLineage.newTagsDatasetFacetFields("pii", "true", "test", null))))
            .datasetType(openLineage.newDatasetTypeDatasetFacet("TABLE", "ICEBERG"))
            .storage(openLineage.newStorageDatasetFacet("s3", "parquet"))
            .catalog(
                openLineage.newCatalogDatasetFacet(
                    "glue",
                    "table",
                    "prod_catalog",
                    "metadata-uri",
                    "warehouse-uri",
                    "crawler",
                    null))
            .version(openLineage.newDatasetVersionDatasetFacet("v7"))
            .lifecycleStateChange(
                openLineage.newLifecycleStateChangeDatasetFacet(
                    OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.DROP, null))
            .build();
    OpenLineage.OutputDataset output =
        openLineage
            .newOutputDatasetBuilder()
            .namespace("snowflake")
            .name("db.schema.customer")
            .facets(datasetFacets)
            .outputFacets(
                openLineage
                    .newOutputDatasetOutputFacetsBuilder()
                    .outputStatistics(
                        openLineage.newOutputStatisticsOutputDatasetFacet(25L, 512L, 3L))
                    .build())
            .build();
    OpenLineage.RunEvent event =
        runEventBuilder(openLineage)
            .eventTime(ZonedDateTime.parse("2026-04-14T10:01:00Z"))
            .outputs(List.of(output))
            .build();

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertRunEventToJob(event, config()).toMcps(config());

    String datasetProperties = aspectJson(mcps, "datasetProperties");
    assertTrue(datasetProperties.contains("Customer output"));
    assertTrue(datasetProperties.contains("https://warehouse.example.com/db/schema/customer"));
    assertTrue(datasetProperties.contains("storageLayer"));
    assertTrue(datasetProperties.contains("s3"));
    assertTrue(datasetProperties.contains("fileFormat"));
    assertTrue(datasetProperties.contains("parquet"));
    assertTrue(datasetProperties.contains("catalogMetadataUri"));
    assertTrue(datasetProperties.contains("metadata-uri"));
    assertTrue(datasetProperties.contains("datasetVersion"));
    assertTrue(datasetProperties.contains("v7"));

    String dataPlatformInstance = aspectJson(mcps, "dataPlatformInstance");
    assertTrue(dataPlatformInstance.contains("prod_catalog"));

    assertAspectsValid(mcps, "operation", Operation::new);
    String operation = aspectJson(mcps, "operation");
    assertTrue(operation.contains("\"numAffectedRows\":25"));
    assertTrue(operation.contains("numAffectedBytes"));
    assertTrue(operation.contains("512"));
    assertTrue(operation.contains("fileCount"));

    assertTrue(aspectJson(mcps, "ownership").contains("alice"));
    assertTrue(aspectJson(mcps, "globalTags").contains("pii"));
    String subTypes = aspectJson(mcps, "subTypes");
    assertTrue(subTypes.contains("TABLE"));
    assertTrue(subTypes.contains("ICEBERG"));
    assertTrue(hasAspectContaining(mcps, "status", "\"removed\":true"));
  }

  @Test
  public void testSameUrnOutputContributionsPreserveOperationsAndDeduplicateRelationships()
      throws Exception {
    OpenLineage openLineage = new OpenLineage(CUSTOM_PRODUCER);
    OpenLineage.OutputDataset firstOutput =
        openLineage
            .newOutputDatasetBuilder()
            .namespace("snowflake")
            .name("db.schema.customer")
            .outputFacets(
                openLineage
                    .newOutputDatasetOutputFacetsBuilder()
                    .outputStatistics(
                        openLineage.newOutputStatisticsOutputDatasetFacet(25L, 512L, 3L))
                    .build())
            .build();
    OpenLineage.OutputDataset secondOutput =
        openLineage
            .newOutputDatasetBuilder()
            .namespace("snowflake")
            .name("db.schema.customer")
            .outputFacets(
                openLineage
                    .newOutputDatasetOutputFacetsBuilder()
                    .outputStatistics(
                        openLineage.newOutputStatisticsOutputDatasetFacet(50L, 1024L, 6L))
                    .build())
            .build();
    OpenLineage.RunEvent event =
        runEventBuilder(openLineage)
            .eventTime(ZonedDateTime.parse("2026-04-14T10:01:00Z"))
            .outputs(List.of(firstOutput, secondOutput))
            .build();

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertRunEventToJob(event, config()).toMcps(config());

    assertEquals(countAspects(mcps, "dataset", "operation"), 2L);
    String operations = allAspectJson(mcps, "operation");
    assertTrue(operations.contains("\"numAffectedRows\":25"));
    assertTrue(operations.contains("\"numAffectedRows\":50"));

    JacksonDataCodec codec = new JacksonDataCodec();
    DataJobInputOutput dataJobInputOutput =
        new DataJobInputOutput(codec.stringToMap(aspectJson(mcps, "dataJobInputOutput")));
    DataProcessInstanceOutput dataProcessInstanceOutput =
        new DataProcessInstanceOutput(
            codec.stringToMap(aspectJson(mcps, "dataProcessInstanceOutput")));
    assertEquals(dataJobInputOutput.getOutputDatasetEdges().size(), 1);
    assertEquals(dataProcessInstanceOutput.getOutputs().size(), 1);
  }

  @Test
  public void testInputStatisticsEmitReadOperation() throws Exception {
    OpenLineage openLineage = new OpenLineage(CUSTOM_PRODUCER);
    OpenLineage.InputDataset input =
        openLineage
            .newInputDatasetBuilder()
            .namespace("snowflake")
            .name("db.schema.customer")
            .inputFacets(
                openLineage
                    .newInputDatasetInputFacetsBuilder()
                    .inputStatistics(
                        openLineage.newInputStatisticsInputDatasetFacet(100L, 2048L, 4L))
                    .build())
            .build();
    OpenLineage.RunEvent event =
        runEventBuilder(openLineage)
            .eventTime(ZonedDateTime.parse("2026-04-14T10:01:00Z"))
            .inputs(List.of(input))
            .build();

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertRunEventToJob(event, config()).toMcps(config());

    String operation = aspectJson(mcps, "operation");
    assertTrue(operation.contains("READ"));
    assertTrue(operation.contains("\"numAffectedRows\":100"));
    assertTrue(operation.contains("numAffectedBytes"));
    assertTrue(operation.contains("2048"));
    assertTrue(operation.contains("fileCount"));
  }

  @Test
  public void testJobSourceAndQueryFacetsEmitTransformAndOutputOperation() throws Exception {
    OpenLineage openLineage = new OpenLineage(CUSTOM_PRODUCER);
    OpenLineage.JobFacets jobFacets =
        openLineage
            .newJobFacetsBuilder()
            .sql(openLineage.newSQLJobFacet("select * from crm.customer", null))
            .sourceCode(openLineage.newSourceCodeJobFacet("python", "print('customer')"))
            .sourceCodeLocation(
                openLineage
                    .newSourceCodeLocationJobFacetBuilder()
                    .type("git")
                    .url(URI.create("https://github.example.com/acme/pipelines/customer.py"))
                    .build())
            .build();
    OpenLineage.OutputDataset output =
        openLineage
            .newOutputDatasetBuilder()
            .namespace("snowflake")
            .name("db.schema.customer")
            .build();
    OpenLineage.RunEvent event =
        runEventBuilder(openLineage)
            .eventTime(ZonedDateTime.parse("2026-04-14T10:01:00Z"))
            .run(
                runBuilder(openLineage)
                    .facets(
                        openLineage
                            .newRunFacetsBuilder()
                            .externalQuery(
                                openLineage.newExternalQueryRunFacet("query-123", "snowflake"))
                            .build())
                    .build())
            .job(jobBuilder(openLineage).facets(jobFacets).build())
            .outputs(List.of(output))
            .build();

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertRunEventToJob(event, config()).toMcps(config());

    String dataJobInfo = aspectJson(mcps, "dataJobInfo");
    assertTrue(dataJobInfo.contains("https://github.example.com/acme/pipelines/customer.py"));
    assertTrue(dataJobInfo.contains("openlineage.sourceCodeLanguage"));
    assertTrue(dataJobInfo.contains("python"));

    String transformLogic = aspectJson(mcps, "dataTransformLogic");
    assertTrue(transformLogic.contains("select * from crm.customer"));
    assertTrue(transformLogic.contains("print('customer')"));

    String operation = aspectJson(mcps, "operation");
    assertTrue(operation.contains("queryStatement"));
    assertTrue(operation.contains("select * from crm.customer"));
    assertTrue(operation.contains("externalQueryId"));
    assertTrue(operation.contains("query-123"));
    assertTrue(operation.contains("externalQuerySource"));
    assertTrue(operation.contains("snowflake"));
  }

  @Test
  public void testJobTypeFacetControlsFlowTypeAndSubTypes() throws Exception {
    OpenLineage openLineage = new OpenLineage(CUSTOM_PRODUCER);
    OpenLineage.JobFacets jobFacets =
        openLineage
            .newJobFacetsBuilder()
            .jobType(openLineage.newJobTypeJobFacet("BATCH", "trino", "SQL"))
            .build();
    OpenLineage.RunEvent event =
        runEventBuilder(openLineage)
            .eventTime(ZonedDateTime.parse("2026-04-14T10:01:00Z"))
            .run(
                runBuilder(openLineage)
                    .facets(
                        openLineage
                            .newRunFacetsBuilder()
                            .processing_engine(
                                openLineage.newProcessingEngineRunFacet("1", "spark", "1.45.0"))
                            .build())
                    .build())
            .job(jobBuilder(openLineage).facets(jobFacets).build())
            .build();

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertRunEventToJob(event, config()).toMcps(config());

    assertEquals(
        OpenLineageToDataHub.convertRunEventToJob(event, config())
            .getFlowUrn()
            .getOrchestratorEntity(),
        "trino");
    assertTrue(aspectJson(mcps, "dataJobInfo").contains("BATCH"));
    assertTrue(aspectJson(mcps, "subTypes").contains("SQL"));
  }

  @Test
  public void testColumnLineageEmitsFineGrainedLineageOnDataJob() throws Exception {
    OpenLineage openLineage = new OpenLineage(CUSTOM_PRODUCER);
    OpenLineage.ColumnLineageDatasetFacetFields fields =
        openLineage
            .newColumnLineageDatasetFacetFieldsBuilder()
            .put(
                "customer_id",
                openLineage.newColumnLineageDatasetFacetFieldsAdditional(
                    List.of(
                        openLineage.newInputField(
                            "snowflake", "db.schema.source", "id", Collections.emptyList())),
                    null,
                    null))
            .build();
    OpenLineage.InputDataset input =
        openLineage
            .newInputDatasetBuilder()
            .namespace("snowflake")
            .name("db.schema.source")
            .build();
    OpenLineage.OutputDataset output =
        openLineage
            .newOutputDatasetBuilder()
            .namespace("snowflake")
            .name("db.schema.customer")
            .facets(
                openLineage
                    .newDatasetFacetsBuilder()
                    .columnLineage(
                        openLineage.newColumnLineageDatasetFacetBuilder().fields(fields).build())
                    .build())
            .build();
    OpenLineage.RunEvent event =
        runEventBuilder(openLineage)
            .eventTime(ZonedDateTime.parse("2026-04-14T10:01:00Z"))
            .inputs(List.of(input))
            .outputs(List.of(output))
            .build();

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertRunEventToJob(event, config()).toMcps(config());

    String dataJobInputOutput = aspectJson(mcps, "dataJobInputOutput");
    assertTrue(dataJobInputOutput.contains("fineGrainedLineages"));
    assertTrue(dataJobInputOutput.contains("db.schema.source"));
    assertTrue(dataJobInputOutput.contains("customer_id"));
    assertEquals(countAspects(mcps, "dataset", "upstreamLineage"), 0L);
  }

  @Test
  public void testDataQualityAssertionsEmitAssertionAndRunEvent() throws Exception {
    OpenLineage openLineage = new OpenLineage(CUSTOM_PRODUCER);
    OpenLineage.InputDataset input =
        openLineage
            .newInputDatasetBuilder()
            .namespace("snowflake")
            .name("db.schema.customer")
            .inputFacets(
                openLineage
                    .newInputDatasetInputFacetsBuilder()
                    .dataQualityAssertions(
                        openLineage.newDataQualityAssertionsDatasetFacet(
                            List.of(
                                openLineage
                                    .newDataQualityAssertionsDatasetFacetAssertionsBuilder()
                                    .assertion("email is not null")
                                    .success(false)
                                    .column("email")
                                    .build(),
                                openLineage
                                    .newDataQualityAssertionsDatasetFacetAssertionsBuilder()
                                    .assertion("id is unique")
                                    .success(true)
                                    .column("id")
                                    .build())))
                    .build())
            .build();
    OpenLineage.RunEvent event =
        runEventBuilder(openLineage)
            .eventTime(ZonedDateTime.parse("2026-04-14T10:01:00Z"))
            .inputs(List.of(input))
            .build();

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertRunEventToJob(event, config()).toMcps(config());

    assertEquals(countAspects(mcps, "assertion", "assertionInfo"), 2);
    assertEquals(countAspects(mcps, "assertion", "assertionRunEvent"), 2);
    assertEquals(countAspects(mcps, "assertion", "dataPlatformInstance"), 2);
    assertAspectsValid(mcps, "assertionRunEvent", AssertionRunEvent::new);
    assertTrue(aspectJson(mcps, "assertionInfo").contains("OpenLineage Data Quality Assertion"));
    assertTrue(aspectJson(mcps, "assertionInfo").contains("email is not null"));
    assertTrue(aspectJson(mcps, "assertionInfo").contains("urn:li:schemaField"));

    String runEvents = allAspectJson(mcps, "assertionRunEvent");
    assertTrue(runEvents.contains("FAILURE"));
    assertTrue(runEvents.contains("SUCCESS"));
    assertTrue(
        runEvents.contains(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customer,PROD)"));

    Set<String> assertionUrns =
        mcps.stream()
            .filter(mcp -> "assertionInfo".equals(mcp.getAspectName()))
            .map(mcp -> mcp.getEntityUrn().toString())
            .collect(Collectors.toSet());
    Set<String> repeatedAssertionUrns =
        OpenLineageToDataHub.convertRunEventToJob(event, config()).toMcps(config()).stream()
            .filter(mcp -> "assertionInfo".equals(mcp.getAspectName()))
            .map(mcp -> mcp.getEntityUrn().toString())
            .collect(Collectors.toSet());
    assertEquals(repeatedAssertionUrns, assertionUrns);
  }

  @Test
  public void testDataQualityMetricsInputDatasetFacetEmitsDatasetProfile() throws Exception {
    OpenLineage openLineage = new OpenLineage(CUSTOM_PRODUCER);
    OpenLineage.DataQualityMetricsInputDatasetFacetColumnMetrics columnMetrics =
        openLineage
            .newDataQualityMetricsInputDatasetFacetColumnMetricsBuilder()
            .put(
                "id",
                openLineage.newDataQualityMetricsInputDatasetFacetColumnMetricsAdditional(
                    1L,
                    99L,
                    4950.0,
                    100.0,
                    1.0,
                    100.0,
                    openLineage
                        .newDataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantilesBuilder()
                        .put("0.5", 50.0)
                        .build()))
            .build();
    OpenLineage.InputDataset input =
        openLineage
            .newInputDatasetBuilder()
            .namespace("snowflake")
            .name("db.schema.customer")
            .inputFacets(
                openLineage
                    .newInputDatasetInputFacetsBuilder()
                    .dataQualityMetrics(
                        openLineage.newDataQualityMetricsInputDatasetFacet(
                            100L, 2048L, 4L, null, columnMetrics))
                    .build())
            .build();
    OpenLineage.RunEvent event =
        runEventBuilder(openLineage)
            .eventTime(ZonedDateTime.parse("2026-04-14T10:01:00Z"))
            .inputs(List.of(input))
            .build();

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertRunEventToJob(event, config()).toMcps(config());
    assertAspectsValid(mcps, "datasetProfile", DatasetProfile::new);
    String datasetProfile = aspectJson(mcps, "datasetProfile");
    assertTrue(datasetProfile.contains("\"rowCount\":100"));
    assertTrue(datasetProfile.contains("\"sizeInBytes\":2048"));
    assertTrue(datasetProfile.contains("\"columnCount\":1"));
    assertTrue(datasetProfile.contains("\"fieldPath\":\"id\""));
    assertTrue(datasetProfile.contains("\"nullCount\":1"));
    assertTrue(datasetProfile.contains("\"uniqueCount\":99"));
    assertTrue(datasetProfile.contains("\"median\":\"50.0\""));
  }

  @Test
  public void testSymlinksDatasetFacetEmitsSiblings() throws Exception {
    OpenLineage openLineage = new OpenLineage(CUSTOM_PRODUCER);
    OpenLineage.DatasetEvent event =
        datasetEventBuilder(openLineage)
            .eventTime(ZonedDateTime.parse("2026-04-14T10:01:00Z"))
            .dataset(
                openLineage
                    .newStaticDatasetBuilder()
                    .namespace("file")
                    .name("/tmp/customer")
                    .facets(
                        openLineage
                            .newDatasetFacetsBuilder()
                            .symlinks(
                                openLineage.newSymlinksDatasetFacet(
                                    List.of(
                                        openLineage.newSymlinksDatasetFacetIdentifiers(
                                            "hive", "table/db/customer", "TABLE"))))
                            .build())
                    .build())
            .build();

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertDatasetEventToMcps(event, config());

    String siblings = aspectJson(mcps, "siblings");
    assertTrue(siblings.contains("urn:li:dataPlatform:file"));
    assertTrue(siblings.contains("/tmp/customer"));
  }

  @Test
  public void testRunTagsAndExtractionErrorFacetPopulateJobAndDpiProperties() throws Exception {
    OpenLineage openLineage = new OpenLineage(CUSTOM_PRODUCER);
    OpenLineage.RunFacets runFacets =
        openLineage
            .newRunFacetsBuilder()
            .tags(
                openLineage.newTagsRunFacet(
                    List.of(
                        openLineage.newTagsRunFacetFields("critical", "true", "test"),
                        openLineage.newTagsRunFacetFields("run-only", "true", "test"))))
            .extractionError(
                openLineage.newExtractionErrorRunFacet(
                    3L,
                    1L,
                    List.of(
                        openLineage.newExtractionErrorRunFacetErrors(
                            "failed to parse", "stack", "task-a", 1L))))
            .build();
    OpenLineage.JobFacets jobFacets =
        openLineage
            .newJobFacetsBuilder()
            .tags(
                openLineage
                    .newTagsJobFacetBuilder()
                    .tags(List.of(openLineage.newTagsJobFacetFields("critical", "true", "test")))
                    .build())
            .build();
    OpenLineage.RunEvent event =
        runEventBuilder(openLineage)
            .eventTime(ZonedDateTime.parse("2026-04-14T10:01:00Z"))
            .run(runBuilder(openLineage).facets(runFacets).build())
            .job(jobBuilder(openLineage).facets(jobFacets).build())
            .build();

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertRunEventToJob(event, config()).toMcps(config());

    String globalTags = aspectJson(mcps, "globalTags");
    assertTrue(globalTags.contains("critical"));
    assertTrue(globalTags.contains("run-only"));
    assertEquals(countOccurrences(globalTags, "critical"), 1);

    String dpiProperties = aspectJson(mcps, "dataProcessInstanceProperties");
    assertTrue(dpiProperties.contains("extractionError.failedTasks"));
    assertTrue(dpiProperties.contains("failed to parse"));

    String runEvent = aspectJson(mcps, "dataProcessInstanceRunEvent");
    assertTrue(runEvent.contains("FAILURE"));
    assertFalse(runEvent.contains("SUCCESS"));
  }

  @Test
  public void testTagsAndOwnershipJobFacetsEmitDataJobAspects() throws Exception {
    OpenLineage openLineage = new OpenLineage(CUSTOM_PRODUCER);
    OpenLineage.JobFacets jobFacets =
        openLineage
            .newJobFacetsBuilder()
            .ownership(
                openLineage
                    .newOwnershipJobFacetBuilder()
                    .owners(
                        List.of(openLineage.newOwnershipJobFacetOwners("alice", "TECHNICAL_OWNER")))
                    .build())
            .tags(
                openLineage
                    .newTagsJobFacetBuilder()
                    .tags(List.of(openLineage.newTagsJobFacetFields("critical", "true", "test")))
                    .build())
            .build();
    OpenLineage.RunEvent event =
        runEventBuilder(openLineage).job(jobBuilder(openLineage).facets(jobFacets).build()).build();

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertRunEventToJob(event, config()).toMcps(config());

    assertEquals(countAspects(mcps, "dataJob", "ownership"), 1L);
    assertEquals(countAspects(mcps, "dataFlow", "ownership"), 0L);
    assertTrue(
        mcps.stream()
            .anyMatch(
                mcp ->
                    "dataJob".equals(mcp.getEntityType())
                        && "globalTags".equals(mcp.getAspectName())));
  }

  @Test
  public void testJobDocumentationTargetsDataJob() throws Exception {
    OpenLineage openLineage = new OpenLineage(CUSTOM_PRODUCER);
    OpenLineage.RunEvent event =
        runEventBuilder(openLineage)
            .eventTime(ZonedDateTime.parse("2026-04-14T10:01:00Z"))
            .job(
                jobBuilder(openLineage)
                    .facets(
                        openLineage
                            .newJobFacetsBuilder()
                            .documentation(
                                openLineage.newDocumentationJobFacet("Load customer docs", null))
                            .build())
                    .build())
            .build();

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertRunEventToJob(event, config()).toMcps(config());

    assertTrue(hasAspectContaining(mcps, "dataJob", "dataJobInfo", "Load customer docs"));
    assertFalse(hasAspectContaining(mcps, "dataFlow", "dataFlowInfo", "Load customer docs"));
  }

  @Test
  public void testJobDependenciesUseCurrentJobOrchestratorForInputDatajobEdges() throws Exception {
    OpenLineage openLineage = new OpenLineage(CUSTOM_PRODUCER);
    OpenLineage dependencyLineage =
        new OpenLineage(
            URI.create("https://github.com/OpenLineage/OpenLineage/integration/airflow"));
    OpenLineage.JobDependency upstream =
        dependencyLineage.newJobDependency(
            dependencyLineage.newJobIdentifier("crm", "extract.customer"),
            dependencyLineage.newRunIdentifier(UUID.randomUUID()),
            "DIRECT_INVOCATION",
            "FINISH_TO_START",
            "EXECUTE_ON_SUCCESS");
    OpenLineage.JobDependency downstream =
        dependencyLineage.newJobDependency(
            dependencyLineage.newJobIdentifier("crm", "publish.customer"),
            null,
            "IMPLICIT_DEPENDENCY",
            null,
            null);
    OpenLineage.JobDependenciesRunFacet dependencies =
        dependencyLineage.newJobDependenciesRunFacet(
            List.of(upstream), List.of(downstream), "ALL_SUCCESS");
    OpenLineage.RunEvent event =
        runEventBuilder(openLineage)
            .eventTime(ZonedDateTime.parse("2026-04-14T10:01:00Z"))
            .run(
                runBuilder(openLineage)
                    .facets(
                        openLineage
                            .newRunFacetsBuilder()
                            .jobDependencies(dependencies)
                            .processing_engine(
                                openLineage.newProcessingEngineRunFacet("476", "trino", "1.45.0"))
                            .build())
                    .build())
            .job(
                jobBuilder(openLineage)
                    .facets(
                        openLineage
                            .newJobFacetsBuilder()
                            .jobType(openLineage.newJobTypeJobFacet("BATCH", "airflow", "SQL"))
                            .build())
                    .build())
            .build();

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertRunEventToJob(event, config()).toMcps(config());

    assertTrue(
        mcps.stream()
            .anyMatch(
                mcp ->
                    "dataJobInfo".equals(mcp.getAspectName())
                        && "urn:li:dataJob:(urn:li:dataFlow:(airflow,load,crm),load.customer)"
                            .equals(mcp.getEntityUrn().toString())));
    String inputOutput = aspectJson(mcps, "dataJobInputOutput");
    assertTrue(inputOutput.contains("inputDatajobEdges"));
    assertTrue(
        inputOutput.contains(
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,extract,crm),extract.customer)"),
        inputOutput);
    assertTrue(inputOutput.contains("DIRECT_INVOCATION"));
    assertTrue(inputOutput.contains("FINISH_TO_START"));
    assertTrue(inputOutput.contains("EXECUTE_ON_SUCCESS"));
    assertFalse(inputOutput.contains("publish.customer"));
    String jobInfo = aspectJson(mcps, "dataJobInfo");
    assertTrue(jobInfo.contains("openlineage.jobDependencies.triggerRule"));
    assertTrue(jobInfo.contains("ALL_SUCCESS"));
    assertTrue(jobInfo.contains("openlineage.jobDependencies.downstream.0.job"));
    assertTrue(
        jobInfo.contains(
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,publish,crm),publish.customer)"));
  }

  @Test
  public void testHierarchyMaterializesContainersWithoutGhostsOrTerminalContainer()
      throws Exception {
    OpenLineage openLineage = new OpenLineage(CUSTOM_PRODUCER);
    OpenLineage.HierarchyDatasetFacet sharedHierarchy =
        openLineage.newHierarchyDatasetFacet(
            List.of(
                openLineage.newHierarchyDatasetFacetLevel("DATABASE", "analytics"),
                openLineage.newHierarchyDatasetFacetLevel("SCHEMA", "sales"),
                openLineage.newHierarchyDatasetFacetLevel("TABLE", "orders")));
    OpenLineage.DatasetFacets facets =
        openLineage.newDatasetFacetsBuilder().hierarchy(sharedHierarchy).build();
    OpenLineage.DatasetFacets copyFacets =
        openLineage
            .newDatasetFacetsBuilder()
            .hierarchy(
                openLineage.newHierarchyDatasetFacet(
                    List.of(
                        openLineage.newHierarchyDatasetFacetLevel("database", "analytics"),
                        openLineage.newHierarchyDatasetFacetLevel("schema", "sales"),
                        openLineage.newHierarchyDatasetFacetLevel("TABLE", "orders_copy"))))
            .build();
    OpenLineage.InputDataset first =
        openLineage
            .newInputDatasetBuilder()
            .namespace("snowflake")
            .name("analytics.sales.orders")
            .facets(facets)
            .build();
    OpenLineage.InputDataset second =
        openLineage
            .newInputDatasetBuilder()
            .namespace("snowflake")
            .name("analytics.sales.orders_copy")
            .facets(copyFacets)
            .build();
    OpenLineage.RunEvent event =
        runEventBuilder(openLineage)
            .eventTime(ZonedDateTime.parse("2026-04-14T10:01:00Z"))
            .inputs(List.of(first, second))
            .build();

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertRunEventToJob(event, config()).toMcps(config());

    assertEquals(countAspects(mcps, "container", "containerKey"), 2L);
    assertEquals(countAspects(mcps, "container", "containerProperties"), 4L);
    assertEquals(countAspects(mcps, "container", "status"), 2L);
    assertEquals(countAspects(mcps, "container", "container"), 1L);
    assertEquals(countAspects(mcps, "dataset", "container"), 2L);
    assertAspectsValid(mcps, "containerProperties", ContainerProperties::new);
    assertAspectsValid(mcps, "container", Container::new);
    assertFalse(allAspectJson(mcps, "containerProperties").contains("orders"));

    String databaseContainer =
        entityUrnForAspectContaining(mcps, "containerProperties", "\"name\":\"analytics\"");
    String schemaContainer =
        entityUrnForAspectContaining(mcps, "containerProperties", "\"name\":\"sales\"");
    assertFalse(hasAspectForEntity(mcps, databaseContainer, "container"));
    assertTrue(aspectJsonForEntity(mcps, schemaContainer, "container").contains(databaseContainer));
    for (String datasetName : List.of("analytics.sales.orders", "analytics.sales.orders_copy")) {
      String datasetUrn = "urn:li:dataset:(urn:li:dataPlatform:snowflake," + datasetName + ",PROD)";
      String datasetContainer = aspectJsonForEntity(mcps, datasetUrn, "container");
      assertTrue(datasetContainer.contains(schemaContainer));
      assertFalse(datasetContainer.contains(databaseContainer));
    }

    Set<String> materializedContainers =
        mcps.stream()
            .filter(mcp -> "container".equals(mcp.getEntityType()))
            .filter(mcp -> "containerKey".equals(mcp.getAspectName()))
            .map(mcp -> mcp.getEntityUrn().toString())
            .collect(Collectors.toSet());
    Set<String> retryContainers =
        OpenLineageToDataHub.convertRunEventToJob(event, config()).toMcps(config()).stream()
            .filter(mcp -> "container".equals(mcp.getEntityType()))
            .filter(mcp -> "containerKey".equals(mcp.getAspectName()))
            .map(mcp -> mcp.getEntityUrn().toString())
            .collect(Collectors.toSet());
    assertEquals(retryContainers, materializedContainers);
    mcps.stream()
        .filter(mcp -> "container".equals(mcp.getAspectName()))
        .map(mcp -> mcp.getAspect().getValue().asString(StandardCharsets.UTF_8))
        .forEach(
            containerAspect ->
                assertTrue(
                    materializedContainers.stream().anyMatch(containerAspect::contains),
                    containerAspect));
  }

  private static <T extends RecordTemplate> void assertAspectsValid(
      List<MetadataChangeProposal> mcps, String aspectName, Function<DataMap, T> templateFactory)
      throws Exception {
    List<MetadataChangeProposal> aspects =
        mcps.stream().filter(mcp -> aspectName.equals(mcp.getAspectName())).toList();
    assertFalse(aspects.isEmpty(), aspectName);
    JacksonDataCodec codec = new JacksonDataCodec();
    for (MetadataChangeProposal mcp : aspects) {
      String json = mcp.getAspect().getValue().asString(StandardCharsets.UTF_8);
      T template = templateFactory.apply(codec.stringToMap(json));
      var result = ValidateDataAgainstSchema.validate(template, new ValidationOptions());
      assertTrue(result.isValid(), aspectName + ": " + result.getMessages());
    }
  }

  private static boolean hasAspect(List<MetadataChangeProposal> mcps, String aspectName) {
    return mcps.stream().anyMatch(mcp -> aspectName.equals(mcp.getAspectName()));
  }

  private static boolean hasAspectForEntity(
      List<MetadataChangeProposal> mcps, String entityUrn, String aspectName) {
    return mcps.stream()
        .anyMatch(
            mcp ->
                entityUrn.equals(mcp.getEntityUrn().toString())
                    && aspectName.equals(mcp.getAspectName()));
  }

  private static String entityUrnForAspectContaining(
      List<MetadataChangeProposal> mcps, String aspectName, String value) {
    return mcps.stream()
        .filter(mcp -> aspectName.equals(mcp.getAspectName()))
        .filter(mcp -> mcp.getAspect().getValue().asString(StandardCharsets.UTF_8).contains(value))
        .map(mcp -> mcp.getEntityUrn().toString())
        .findFirst()
        .orElseThrow();
  }

  private static String aspectJsonForEntity(
      List<MetadataChangeProposal> mcps, String entityUrn, String aspectName) {
    return mcps.stream()
        .filter(mcp -> entityUrn.equals(mcp.getEntityUrn().toString()))
        .filter(mcp -> aspectName.equals(mcp.getAspectName()))
        .findFirst()
        .orElseThrow()
        .getAspect()
        .getValue()
        .asString(StandardCharsets.UTF_8);
  }

  private static long countAspects(
      List<MetadataChangeProposal> mcps, String entityType, String aspectName) {
    return mcps.stream()
        .filter(mcp -> entityType.equals(mcp.getEntityType()))
        .filter(mcp -> aspectName.equals(mcp.getAspectName()))
        .count();
  }

  private static boolean hasAspectContaining(
      List<MetadataChangeProposal> mcps, String aspectName, String value) {
    return mcps.stream()
        .filter(mcp -> aspectName.equals(mcp.getAspectName()))
        .anyMatch(
            mcp -> mcp.getAspect().getValue().asString(StandardCharsets.UTF_8).contains(value));
  }

  private static boolean hasAspectContaining(
      List<MetadataChangeProposal> mcps, String entityType, String aspectName, String value) {
    return mcps.stream()
        .filter(mcp -> entityType.equals(mcp.getEntityType()))
        .filter(mcp -> aspectName.equals(mcp.getAspectName()))
        .anyMatch(
            mcp -> mcp.getAspect().getValue().asString(StandardCharsets.UTF_8).contains(value));
  }

  private static String aspectJson(List<MetadataChangeProposal> mcps, String aspectName) {
    return mcps.stream()
        .filter(mcp -> aspectName.equals(mcp.getAspectName()))
        .findFirst()
        .orElseThrow()
        .getAspect()
        .getValue()
        .asString(StandardCharsets.UTF_8);
  }

  private static String allAspectJson(List<MetadataChangeProposal> mcps, String aspectName) {
    return mcps.stream()
        .filter(mcp -> aspectName.equals(mcp.getAspectName()))
        .map(mcp -> mcp.getAspect().getValue().asString(StandardCharsets.UTF_8))
        .collect(Collectors.joining("\n"));
  }

  private static int countOccurrences(String value, String search) {
    int count = 0;
    int index = 0;
    while ((index = value.indexOf(search, index)) >= 0) {
      count++;
      index += search.length();
    }
    return count;
  }
}
