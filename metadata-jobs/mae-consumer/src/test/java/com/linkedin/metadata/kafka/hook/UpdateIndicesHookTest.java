package com.linkedin.metadata.kafka.hook;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.kafka.hook.MCLProcessingTestDataGenerator.*;
import static com.linkedin.metadata.search.utils.QueryUtils.newRelationshipFilter;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.InputField;
import com.linkedin.common.InputFieldArray;
import com.linkedin.common.InputFields;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.dataset.FineGrainedLineage;
import com.linkedin.dataset.FineGrainedLineageArray;
import com.linkedin.dataset.FineGrainedLineageDownstreamType;
import com.linkedin.dataset.FineGrainedLineageUpstreamType;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.boot.kafka.DataHubUpgradeKafkaListener;
import com.linkedin.metadata.config.SystemUpdateConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.key.ChartKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.service.UpdateGraphIndicesService;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.schema.NumberType;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.structured.StructuredPropertyDefinition;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateIndicesHookTest {
  //  going to want a test where we have an upstreamLineage aspect with finegrained, check that we
  // call _graphService.addEdge for each edge
  //  as well as _graphService.removeEdgesFromNode for each field and their relationships

  static final long EVENT_TIME = 123L;
  static final String TEST_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)";
  static final String TEST_DATASET_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsDataset,PROD)";
  static final String TEST_DATASET_URN_3 =
      "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressKafkaDataset,PROD)";
  static final String TEST_CHART_URN = "urn:li:chart:(looker,dashboard_elements.1)";
  static final String TEST_ACTOR_URN = "urn:li:corpuser:test";
  static final String DOWNSTREAM_OF = "DownstreamOf";
  static final String RUN_ID_1 = "123";
  static final String RUN_ID_2 = "456";
  static final String RUN_ID_3 = "789";
  static final long LAST_OBSERVED_1 = 123L;
  static final long LAST_OBSERVED_2 = 456L;
  static final long LAST_OBSERVED_3 = 789L;
  private UpdateIndicesHook updateIndicesHook;
  private GraphService mockGraphService;
  private EntitySearchService mockEntitySearchService;
  private TimeseriesAspectService mockTimeseriesAspectService;
  private SystemMetadataService mockSystemMetadataService;
  private SearchDocumentTransformer searchDocumentTransformer;
  private DataHubUpgradeKafkaListener mockDataHubUpgradeKafkaListener;
  private ConfigurationProvider mockConfigurationProvider;
  private EntityIndexBuilders mockEntityIndexBuilders;
  private Urn actorUrn;
  private UpdateIndicesService updateIndicesService;
  private UpdateIndicesHook reprocessUIHook;
  private OperationContext opContext;

  @Value("${elasticsearch.index.maxArrayLength}")
  private int maxArrayLength;

  @BeforeMethod
  public void setupTest() {
    actorUrn = UrnUtils.getUrn(TEST_ACTOR_URN);
    mockGraphService = mock(ElasticSearchGraphService.class);
    mockEntitySearchService = mock(EntitySearchService.class);
    mockTimeseriesAspectService = mock(TimeseriesAspectService.class);
    mockSystemMetadataService = mock(SystemMetadataService.class);
    searchDocumentTransformer = new SearchDocumentTransformer(1000, 1000, 1000);
    mockDataHubUpgradeKafkaListener = mock(DataHubUpgradeKafkaListener.class);
    mockConfigurationProvider = mock(ConfigurationProvider.class);
    mockEntityIndexBuilders = mock(EntityIndexBuilders.class);

    when(mockEntityIndexBuilders.getIndexConvention()).thenReturn(IndexConventionImpl.noPrefix(""));

    ElasticSearchConfiguration elasticSearchConfiguration = new ElasticSearchConfiguration();
    SystemUpdateConfiguration systemUpdateConfiguration = new SystemUpdateConfiguration();
    systemUpdateConfiguration.setWaitForSystemUpdate(false);
    when(mockConfigurationProvider.getElasticSearch()).thenReturn(elasticSearchConfiguration);
    updateIndicesService =
        new UpdateIndicesService(
            UpdateGraphIndicesService.withService(mockGraphService),
            mockEntitySearchService,
            mockTimeseriesAspectService,
            mockSystemMetadataService,
            searchDocumentTransformer,
            mockEntityIndexBuilders,
            "MD5");

    opContext = TestOperationContexts.systemContextNoSearchAuthorization();

    updateIndicesHook = new UpdateIndicesHook(updateIndicesService, true, false);
    updateIndicesHook.init(opContext);
    reprocessUIHook = new UpdateIndicesHook(updateIndicesService, true, true);
    reprocessUIHook.init(opContext);
  }

  @Test
  public void testFineGrainedLineageEdgesAreAdded() throws Exception {
    updateIndicesService.getUpdateGraphIndicesService().setGraphDiffMode(false);
    Urn upstreamUrn =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsDataset,PROD),foo_info)");
    Urn downstreamUrn =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD),field_foo)");
    Urn lifeCycleOwner =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)");
    MetadataChangeLog event = createUpstreamLineageMCL(upstreamUrn, downstreamUrn);
    updateIndicesHook.invoke(event);

    Edge edge =
        new Edge(
            downstreamUrn,
            upstreamUrn,
            DOWNSTREAM_OF,
            null,
            null,
            null,
            null,
            null,
            lifeCycleOwner,
            null);
    Mockito.verify(mockGraphService, Mockito.times(1)).addEdge(Mockito.eq(edge));
    Mockito.verify(mockGraphService, Mockito.times(1))
        .removeEdgesFromNode(
            any(OperationContext.class),
            Mockito.eq(downstreamUrn),
            Mockito.eq(new ArrayList<>(Collections.singleton(DOWNSTREAM_OF))),
            Mockito.eq(
                newRelationshipFilter(
                    new Filter().setOr(new ConjunctiveCriterionArray()),
                    RelationshipDirection.OUTGOING)));
  }

  @Test
  public void testFineGrainedLineageEdgesAreAddedRestate() throws Exception {
    updateIndicesService.getUpdateGraphIndicesService().setGraphDiffMode(false);
    Urn upstreamUrn =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsDataset,PROD),foo_info)");
    Urn downstreamUrn =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD),field_foo)");
    Urn lifeCycleOwner =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)");
    MetadataChangeLog event =
        createUpstreamLineageMCL(upstreamUrn, downstreamUrn, ChangeType.RESTATE);
    updateIndicesHook.invoke(event);

    Edge edge =
        new Edge(
            downstreamUrn,
            upstreamUrn,
            DOWNSTREAM_OF,
            null,
            null,
            null,
            null,
            null,
            lifeCycleOwner,
            null);
    Mockito.verify(mockGraphService, Mockito.times(1)).addEdge(Mockito.eq(edge));
    Mockito.verify(mockGraphService, Mockito.times(1))
        .removeEdgesFromNode(
            any(OperationContext.class),
            Mockito.eq(downstreamUrn),
            Mockito.eq(new ArrayList<>(Collections.singleton(DOWNSTREAM_OF))),
            Mockito.eq(
                newRelationshipFilter(
                    new Filter().setOr(new ConjunctiveCriterionArray()),
                    RelationshipDirection.OUTGOING)));
    Mockito.verify(mockEntitySearchService, Mockito.times(1))
        .upsertDocument(
            any(OperationContext.class),
            Mockito.eq(DATASET_ENTITY_NAME),
            Mockito.any(),
            Mockito.eq(
                URLEncoder.encode(
                    "urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)",
                    StandardCharsets.UTF_8)));
  }

  @Test
  public void testInputFieldsEdgesAreAdded() throws Exception {
    Urn upstreamUrn =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:looker,thelook.explore.order_items,PROD),users.count)");
    String downstreamFieldPath = "users.count";
    MetadataChangeLog event = createInputFieldsMCL(upstreamUrn, downstreamFieldPath);
    EntityRegistry mockEntityRegistry = createMockEntityRegistry();
    updateIndicesService =
        new UpdateIndicesService(
            new UpdateGraphIndicesService(mockGraphService, false, true),
            mockEntitySearchService,
            mockTimeseriesAspectService,
            mockSystemMetadataService,
            searchDocumentTransformer,
            mockEntityIndexBuilders,
            "MD5");

    updateIndicesHook = new UpdateIndicesHook(updateIndicesService, true, false);
    updateIndicesHook.init(
        TestOperationContexts.userContextNoSearchAuthorization(mockEntityRegistry));

    updateIndicesHook.invoke(event);

    Urn downstreamUrn =
        UrnUtils.getUrn(
            String.format("urn:li:schemaField:(%s,%s)", TEST_CHART_URN, downstreamFieldPath));

    Edge edge = new Edge(downstreamUrn, upstreamUrn, DOWNSTREAM_OF, null, null, null, null, null);
    Mockito.verify(mockGraphService, Mockito.times(1)).addEdge(Mockito.eq(edge));
    Mockito.verify(mockGraphService, Mockito.times(1))
        .removeEdgesFromNode(
            any(OperationContext.class),
            Mockito.eq(downstreamUrn),
            Mockito.eq(new ArrayList<>(Collections.singleton(DOWNSTREAM_OF))),
            Mockito.eq(
                newRelationshipFilter(
                    new Filter().setOr(new ConjunctiveCriterionArray()),
                    RelationshipDirection.OUTGOING)));
  }

  @Test
  public void testMCLProcessExhaustive() throws URISyntaxException {
    /*
     * newLineage
     */
    MetadataChangeLog changeLog = createBaseChangeLog();

    updateIndicesHook.invoke(changeLog);

    // One new edge added
    Mockito.verify(mockGraphService, Mockito.times(1)).addEdge(Mockito.any());
    // Update document
    Mockito.verify(mockEntitySearchService, Mockito.times(1))
        .upsertDocument(
            any(OperationContext.class),
            Mockito.eq(DATASET_ENTITY_NAME),
            Mockito.any(),
            Mockito.eq(URLEncoder.encode(TEST_DATASET_URN, StandardCharsets.UTF_8)));

    /*
     * restateNewLineage
     */
    Mockito.clearInvocations(mockGraphService, mockEntitySearchService);
    changeLog = setSystemMetadata(changeLog);
    changeLog = setPreviousData(setToRestate(changeLog), changeLog);

    updateIndicesHook.invoke(changeLog);

    // No edges added
    Mockito.verify(mockGraphService, Mockito.times(0)).addEdge(Mockito.any());
    // Timestamp updated
    Mockito.verify(mockGraphService, Mockito.times(1)).upsertEdge(Mockito.any());
    // No document change
    Mockito.verify(mockEntitySearchService, Mockito.times(0))
        .upsertDocument(any(OperationContext.class), Mockito.any(), Mockito.any(), Mockito.any());

    /*
     * addLineage
     */
    Mockito.clearInvocations(mockGraphService, mockEntitySearchService);
    changeLog = setPreviousData(setToUpsert(changeLog), changeLog);
    UpstreamLineage currentLineage = addLineageEdge(createBaseLineageAspect());
    changeLog = modifyAspect(changeLog, currentLineage);

    updateIndicesHook.invoke(changeLog);

    // New edge added
    Mockito.verify(mockGraphService, Mockito.times(1)).addEdge(Mockito.any());
    // Update timestamp of old edge
    Mockito.verify(mockGraphService, Mockito.times(1)).upsertEdge(Mockito.any());
    // Document update for new upstream
    Mockito.verify(mockEntitySearchService, Mockito.times(1))
        .upsertDocument(any(OperationContext.class), Mockito.any(), Mockito.any(), Mockito.any());

    /*
     * restateAddLineage
     */
    Mockito.clearInvocations(mockGraphService, mockEntitySearchService);
    changeLog = setPreviousData(setToRestate(changeLog), changeLog);

    updateIndicesHook.invoke(changeLog);

    // No new edges
    Mockito.verify(mockGraphService, Mockito.times(0)).addEdge(Mockito.any());
    // Update timestamps of old edges
    Mockito.verify(mockGraphService, Mockito.times(2)).upsertEdge(Mockito.any());
    // No document update
    Mockito.verify(mockEntitySearchService, Mockito.times(0))
        .upsertDocument(any(OperationContext.class), Mockito.any(), Mockito.any(), Mockito.any());

    /*
     * noOpUpsert
     */
    Mockito.clearInvocations(mockGraphService, mockEntitySearchService);
    changeLog = setPreviousData(setToUpsert(changeLog), changeLog);

    updateIndicesHook.invoke(changeLog);

    // No new edges
    Mockito.verify(mockGraphService, Mockito.times(0)).addEdge(Mockito.any());
    // Update timestamps of old edges
    Mockito.verify(mockGraphService, Mockito.times(2)).upsertEdge(Mockito.any());
    // No document update
    Mockito.verify(mockEntitySearchService, Mockito.times(0))
        .upsertDocument(any(OperationContext.class), Mockito.any(), Mockito.any(), Mockito.any());

    /*
     * restateNoOp
     */
    Mockito.clearInvocations(mockGraphService, mockEntitySearchService);
    changeLog = setPreviousData(setToRestate(changeLog), changeLog);

    updateIndicesHook.invoke(changeLog);

    // No new edges
    Mockito.verify(mockGraphService, Mockito.times(0)).addEdge(Mockito.any());
    // Update timestamps of old edges
    Mockito.verify(mockGraphService, Mockito.times(2)).upsertEdge(Mockito.any());
    // No document update
    Mockito.verify(mockEntitySearchService, Mockito.times(0))
        .upsertDocument(any(OperationContext.class), Mockito.any(), Mockito.any(), Mockito.any());

    /*
     * systemMetadataChange
     */
    Mockito.clearInvocations(mockGraphService, mockEntitySearchService);
    changeLog = setPreviousData(setToUpsert(changeLog), changeLog);
    changeLog = modifySystemMetadata(changeLog);

    updateIndicesHook.invoke(changeLog);

    // No new edges
    Mockito.verify(mockGraphService, Mockito.times(0)).addEdge(Mockito.any());
    // Update timestamps of old edges
    Mockito.verify(mockGraphService, Mockito.times(2)).upsertEdge(Mockito.any());
    // No document update
    Mockito.verify(mockEntitySearchService, Mockito.times(0))
        .upsertDocument(any(OperationContext.class), Mockito.any(), Mockito.any(), Mockito.any());

    /*
     * restateSystemMetadataChange
     */
    Mockito.clearInvocations(mockGraphService, mockEntitySearchService);
    changeLog = setPreviousData(setToRestate(changeLog), changeLog);
    changeLog = modifySystemMetadata2(changeLog);

    updateIndicesHook.invoke(changeLog);

    // No new edges
    Mockito.verify(mockGraphService, Mockito.times(0)).addEdge(Mockito.any());
    // Update timestamps of old edges
    Mockito.verify(mockGraphService, Mockito.times(2)).upsertEdge(Mockito.any());
    // No document update
    Mockito.verify(mockEntitySearchService, Mockito.times(0))
        .upsertDocument(any(OperationContext.class), Mockito.any(), Mockito.any(), Mockito.any());

    /*
     * modifyNonSearchableField
     */
    Mockito.clearInvocations(mockGraphService, mockEntitySearchService);
    changeLog = setPreviousData(setToUpsert(changeLog), changeLog);
    currentLineage = modifyNonSearchableField(currentLineage);
    changeLog = modifyAspect(changeLog, currentLineage);

    updateIndicesHook.invoke(changeLog);

    // No new edges
    Mockito.verify(mockGraphService, Mockito.times(0)).addEdge(Mockito.any());
    // Update timestamps of old edges
    Mockito.verify(mockGraphService, Mockito.times(2)).upsertEdge(Mockito.any());
    // No document update
    Mockito.verify(mockEntitySearchService, Mockito.times(0))
        .upsertDocument(any(OperationContext.class), Mockito.any(), Mockito.any(), Mockito.any());

    /*
     * force reindexing
     */
    Mockito.clearInvocations(mockGraphService, mockEntitySearchService);
    changeLog = setPreviousDataToEmpty(setToRestate(changeLog));
    changeLog = setSystemMetadataWithForceIndexing(changeLog);

    updateIndicesHook.invoke(changeLog);

    // Forced removal of all edges
    Mockito.verify(mockGraphService, Mockito.times(1))
        .removeEdgesFromNode(any(OperationContext.class), any(), any(), any());
    // Forced add of edges
    Mockito.verify(mockGraphService, Mockito.times(2)).addEdge(Mockito.any());
    // Forced document update
    Mockito.verify(mockEntitySearchService, Mockito.times(1))
        .upsertDocument(any(OperationContext.class), Mockito.any(), Mockito.any(), Mockito.any());
  }

  @Test
  public void testMCLUIPreProcessed() throws Exception {
    Urn upstreamUrn =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsDataset,PROD),foo_info)");
    Urn downstreamUrn =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD),field_foo)");

    MetadataChangeLog changeLog =
        createUpstreamLineageMCLUIPreProcessed(upstreamUrn, downstreamUrn, ChangeType.UPSERT);
    updateIndicesHook.invoke(changeLog);
    Mockito.verifyNoInteractions(
        mockEntitySearchService,
        mockGraphService,
        mockTimeseriesAspectService,
        mockSystemMetadataService);
  }

  @Test
  public void testMCLUIPreProcessedReprocess() throws Exception {
    Urn upstreamUrn =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsDataset,PROD),foo_info2)");
    Urn downstreamUrn =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD),field_foo2)");

    MetadataChangeLog changeLog =
        createUpstreamLineageMCLUIPreProcessed(upstreamUrn, downstreamUrn, ChangeType.UPSERT);
    reprocessUIHook.invoke(changeLog);
    Mockito.verify(mockGraphService, Mockito.times(3)).addEdge(Mockito.any());
    Mockito.verify(mockEntitySearchService, Mockito.times(1))
        .upsertDocument(any(OperationContext.class), Mockito.any(), Mockito.any(), Mockito.any());
  }

  @Test
  public void testUpdateIndexMappings() throws CloneNotSupportedException {
    // ensure no mutation
    EntitySpec entitySpec =
        opContext.getEntityRegistry().getEntitySpec(STRUCTURED_PROPERTY_ENTITY_NAME);
    AspectSpec aspectSpec = entitySpec.getAspectSpec(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME);

    StructuredPropertyDefinition oldValueOrigin =
        new StructuredPropertyDefinition()
            .setEntityTypes(
                new UrnArray(
                    UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,foo1,PROD)"),
                    UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,foo2,PROD)")));
    StructuredPropertyDefinition newValueOrigin =
        new StructuredPropertyDefinition()
            .setEntityTypes(
                new UrnArray(
                    UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,foo2,PROD)"),
                    UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,foo3,PROD)")));

    StructuredPropertyDefinition oldValue =
        new StructuredPropertyDefinition(oldValueOrigin.data().copy());
    StructuredPropertyDefinition newValue =
        new StructuredPropertyDefinition(newValueOrigin.data().copy());

    updateIndicesService.updateIndexMappings(
        UrnUtils.getUrn(TEST_DATASET_URN), entitySpec, aspectSpec, newValue, oldValue);

    assertEquals(oldValue, oldValueOrigin, "Ensure no mutation to input objects");
    assertEquals(newValue, newValueOrigin, "Ensure no mutation to input objects");
  }

  private EntityRegistry createMockEntityRegistry() {
    // need to mock this registry instead of using test-entity-registry.yml because inputFields does
    // not work due to a known bug
    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);
    EntitySpec entitySpec = mock(EntitySpec.class);
    AspectSpec aspectSpec = createMockAspectSpec(InputFields.class, InputFields.dataSchema());
    AspectSpec upstreamLineageAspectSpec =
        createMockAspectSpec(UpstreamLineage.class, UpstreamLineage.dataSchema());
    when(mockEntityRegistry.getEntitySpec(Constants.CHART_ENTITY_NAME)).thenReturn(entitySpec);
    when(mockEntityRegistry.getEntitySpec(Constants.DATASET_ENTITY_NAME)).thenReturn(entitySpec);
    when(mockEntityRegistry.getEntitySpec(SCHEMA_FIELD_ENTITY_NAME)).thenReturn(entitySpec);
    when(mockEntityRegistry.getEntitySpec(DATA_PLATFORM_ENTITY_NAME)).thenReturn(entitySpec);
    when(entitySpec.getAspectSpec(Constants.INPUT_FIELDS_ASPECT_NAME)).thenReturn(aspectSpec);
    when(entitySpec.getAspectSpec(Constants.UPSTREAM_LINEAGE_ASPECT_NAME))
        .thenReturn(upstreamLineageAspectSpec);
    when(aspectSpec.isTimeseries()).thenReturn(false);
    when(aspectSpec.getName()).thenReturn(Constants.INPUT_FIELDS_ASPECT_NAME);
    when(upstreamLineageAspectSpec.isTimeseries()).thenReturn(false);
    when(upstreamLineageAspectSpec.getName()).thenReturn(Constants.UPSTREAM_LINEAGE_ASPECT_NAME);
    AspectSpec chartKeyAspectSpec = createMockAspectSpec(ChartKey.class, ChartKey.dataSchema());
    when(entitySpec.getKeyAspectSpec()).thenReturn(chartKeyAspectSpec);
    return mockEntityRegistry;
  }

  private <T extends RecordTemplate> AspectSpec createMockAspectSpec(
      Class<T> clazz, RecordDataSchema schema) {
    AspectSpec mockSpec = mock(AspectSpec.class);
    when(mockSpec.getDataTemplateClass()).thenReturn((Class<RecordTemplate>) clazz);
    when(mockSpec.getPegasusSchema()).thenReturn(schema);
    return mockSpec;
  }

  private MetadataChangeLog createUpstreamLineageMCL(Urn upstreamUrn, Urn downstreamUrn)
      throws Exception {
    return createUpstreamLineageMCL(upstreamUrn, downstreamUrn, ChangeType.UPSERT);
  }

  private MetadataChangeLog createUpstreamLineageMCL(
      Urn upstreamUrn, Urn downstreamUrn, ChangeType changeType) throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(Constants.DATASET_ENTITY_NAME);
    event.setAspectName(Constants.UPSTREAM_LINEAGE_ASPECT_NAME);
    event.setChangeType(changeType);

    UpstreamLineage upstreamLineage = new UpstreamLineage();
    FineGrainedLineageArray fineGrainedLineages = new FineGrainedLineageArray();
    FineGrainedLineage fineGrainedLineage = new FineGrainedLineage();
    fineGrainedLineage.setDownstreamType(FineGrainedLineageDownstreamType.FIELD);
    fineGrainedLineage.setUpstreamType(FineGrainedLineageUpstreamType.DATASET);
    UrnArray upstreamUrns = new UrnArray();
    upstreamUrns.add(upstreamUrn);
    fineGrainedLineage.setUpstreams(upstreamUrns);
    UrnArray downstreamUrns = new UrnArray();
    downstreamUrns.add(downstreamUrn);
    fineGrainedLineage.setDownstreams(downstreamUrns);
    fineGrainedLineages.add(fineGrainedLineage);
    upstreamLineage.setFineGrainedLineages(fineGrainedLineages);
    final UpstreamArray upstreamArray = new UpstreamArray();
    final Upstream upstream = new Upstream();
    upstream.setType(DatasetLineageType.TRANSFORMED);
    upstream.setDataset(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsDataset,PROD)"));
    upstreamArray.add(upstream);
    upstreamLineage.setUpstreams(upstreamArray);

    event.setAspect(GenericRecordUtils.serializeAspect(upstreamLineage));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(actorUrn).setTime(EVENT_TIME));
    return event;
  }

  private MetadataChangeLog createUpstreamLineageMCLUIPreProcessed(
      Urn upstreamUrn, Urn downstreamUrn, ChangeType changeType) throws Exception {
    final MetadataChangeLog metadataChangeLog =
        createUpstreamLineageMCL(upstreamUrn, downstreamUrn, changeType);
    final StringMap properties = new StringMap();
    properties.put(APP_SOURCE, UI_SOURCE);
    final SystemMetadata systemMetadata = new SystemMetadata().setProperties(properties);
    metadataChangeLog.setSystemMetadata(systemMetadata);
    return metadataChangeLog;
  }

  private MetadataChangeLog createInputFieldsMCL(Urn upstreamUrn, String downstreamFieldPath)
      throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(Constants.CHART_ENTITY_NAME);
    event.setAspectName(Constants.INPUT_FIELDS_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    InputFields inputFields = new InputFields();
    InputFieldArray inputFieldsArray = new InputFieldArray();
    InputField inputField = new InputField();
    inputField.setSchemaFieldUrn(upstreamUrn);
    SchemaField schemaField = new SchemaField();
    schemaField.setFieldPath(downstreamFieldPath);
    schemaField.setNativeDataType("int");
    schemaField.setType(
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())));
    inputField.setSchemaField(schemaField);
    inputFieldsArray.add(inputField);
    inputFields.setFields(inputFieldsArray);

    event.setAspect(GenericRecordUtils.serializeAspect(inputFields));
    event.setEntityUrn(Urn.createFromString(TEST_CHART_URN));
    event.setEntityType(Constants.CHART_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(actorUrn).setTime(EVENT_TIME));
    return event;
  }
}
