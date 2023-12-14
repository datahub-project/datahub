package com.linkedin.metadata.kafka.hook;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.kafka.hook.EntityRegistryTestUtil.ENTITY_REGISTRY;
import static com.linkedin.metadata.kafka.hook.MCLProcessingTestDataGenerator.*;
import static com.linkedin.metadata.search.utils.QueryUtils.newRelationshipFilter;

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
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.kafka.DataHubUpgradeKafkaListener;
import com.linkedin.metadata.config.SystemUpdateConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.graph.Edge;
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
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.schema.SchemaField;
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
  private UpdateIndicesHook _updateIndicesHook;
  private GraphService _mockGraphService;
  private EntitySearchService _mockEntitySearchService;
  private TimeseriesAspectService _mockTimeseriesAspectService;
  private SystemMetadataService _mockSystemMetadataService;
  private SearchDocumentTransformer _searchDocumentTransformer;
  private DataHubUpgradeKafkaListener _mockDataHubUpgradeKafkaListener;
  private ConfigurationProvider _mockConfigurationProvider;
  private EntityIndexBuilders _mockEntityIndexBuilders;
  private Urn _actorUrn;
  private UpdateIndicesService _updateIndicesService;

  @Value("${elasticsearch.index.maxArrayLength}")
  private int maxArrayLength;

  @BeforeMethod
  public void setupTest() {
    _actorUrn = UrnUtils.getUrn(TEST_ACTOR_URN);
    _mockGraphService = Mockito.mock(ElasticSearchGraphService.class);
    _mockEntitySearchService = Mockito.mock(EntitySearchService.class);
    _mockTimeseriesAspectService = Mockito.mock(TimeseriesAspectService.class);
    _mockSystemMetadataService = Mockito.mock(SystemMetadataService.class);
    _searchDocumentTransformer = new SearchDocumentTransformer(1000, 1000, 1000);
    _mockDataHubUpgradeKafkaListener = Mockito.mock(DataHubUpgradeKafkaListener.class);
    _mockConfigurationProvider = Mockito.mock(ConfigurationProvider.class);
    _mockEntityIndexBuilders = Mockito.mock(EntityIndexBuilders.class);

    ElasticSearchConfiguration elasticSearchConfiguration = new ElasticSearchConfiguration();
    SystemUpdateConfiguration systemUpdateConfiguration = new SystemUpdateConfiguration();
    systemUpdateConfiguration.setWaitForSystemUpdate(false);
    Mockito.when(_mockConfigurationProvider.getElasticSearch())
        .thenReturn(elasticSearchConfiguration);
    _updateIndicesService =
        new UpdateIndicesService(
            _mockGraphService,
            _mockEntitySearchService,
            _mockTimeseriesAspectService,
            _mockSystemMetadataService,
            ENTITY_REGISTRY,
            _searchDocumentTransformer,
            _mockEntityIndexBuilders);
    _updateIndicesHook = new UpdateIndicesHook(_updateIndicesService, true);
  }

  @Test
  public void testFineGrainedLineageEdgesAreAdded() throws Exception {
    _updateIndicesService.setGraphDiffMode(false);
    Urn upstreamUrn =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsDataset,PROD),foo_info)");
    Urn downstreamUrn =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD),field_foo)");
    MetadataChangeLog event = createUpstreamLineageMCL(upstreamUrn, downstreamUrn);
    _updateIndicesHook.invoke(event);

    Edge edge = new Edge(downstreamUrn, upstreamUrn, DOWNSTREAM_OF, null, null, null, null, null);
    Mockito.verify(_mockGraphService, Mockito.times(1)).addEdge(Mockito.eq(edge));
    Mockito.verify(_mockGraphService, Mockito.times(1))
        .removeEdgesFromNode(
            Mockito.eq(downstreamUrn),
            Mockito.eq(new ArrayList<>(Collections.singleton(DOWNSTREAM_OF))),
            Mockito.eq(
                newRelationshipFilter(
                    new Filter().setOr(new ConjunctiveCriterionArray()),
                    RelationshipDirection.OUTGOING)));
  }

  @Test
  public void testFineGrainedLineageEdgesAreAddedRestate() throws Exception {
    _updateIndicesService.setGraphDiffMode(false);
    Urn upstreamUrn =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsDataset,PROD),foo_info)");
    Urn downstreamUrn =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD),field_foo)");
    MetadataChangeLog event =
        createUpstreamLineageMCL(upstreamUrn, downstreamUrn, ChangeType.RESTATE);
    _updateIndicesHook.invoke(event);

    Edge edge = new Edge(downstreamUrn, upstreamUrn, DOWNSTREAM_OF, null, null, null, null, null);
    Mockito.verify(_mockGraphService, Mockito.times(1)).addEdge(Mockito.eq(edge));
    Mockito.verify(_mockGraphService, Mockito.times(1))
        .removeEdgesFromNode(
            Mockito.eq(downstreamUrn),
            Mockito.eq(new ArrayList<>(Collections.singleton(DOWNSTREAM_OF))),
            Mockito.eq(
                newRelationshipFilter(
                    new Filter().setOr(new ConjunctiveCriterionArray()),
                    RelationshipDirection.OUTGOING)));
    Mockito.verify(_mockEntitySearchService, Mockito.times(1))
        .upsertDocument(
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
    _updateIndicesService =
        new UpdateIndicesService(
            _mockGraphService,
            _mockEntitySearchService,
            _mockTimeseriesAspectService,
            _mockSystemMetadataService,
            mockEntityRegistry,
            _searchDocumentTransformer,
            _mockEntityIndexBuilders);
    _updateIndicesHook = new UpdateIndicesHook(_updateIndicesService, true);

    _updateIndicesHook.invoke(event);

    Urn downstreamUrn =
        UrnUtils.getUrn(
            String.format("urn:li:schemaField:(%s,%s)", TEST_CHART_URN, downstreamFieldPath));

    Edge edge = new Edge(downstreamUrn, upstreamUrn, DOWNSTREAM_OF, null, null, null, null, null);
    Mockito.verify(_mockGraphService, Mockito.times(1)).addEdge(Mockito.eq(edge));
    Mockito.verify(_mockGraphService, Mockito.times(1))
        .removeEdgesFromNode(
            Mockito.eq(downstreamUrn),
            Mockito.eq(new ArrayList<>(Collections.singleton(DOWNSTREAM_OF))),
            Mockito.eq(
                newRelationshipFilter(
                    new Filter().setOr(new ConjunctiveCriterionArray()),
                    RelationshipDirection.OUTGOING)));
  }

  @Test
  public void testMCLProcessExhaustive() throws URISyntaxException {

    _updateIndicesService.setGraphDiffMode(true);
    _updateIndicesService.setSearchDiffMode(true);
    /*
     * newLineage
     */
    MetadataChangeLog changeLog = createBaseChangeLog();

    _updateIndicesHook.invoke(changeLog);

    // One new edge added
    Mockito.verify(_mockGraphService, Mockito.times(1)).addEdge(Mockito.any());
    // Update document
    Mockito.verify(_mockEntitySearchService, Mockito.times(1))
        .upsertDocument(
            Mockito.eq(DATASET_ENTITY_NAME),
            Mockito.any(),
            Mockito.eq(URLEncoder.encode(TEST_DATASET_URN, StandardCharsets.UTF_8)));

    /*
     * restateNewLineage
     */
    Mockito.clearInvocations(_mockGraphService, _mockEntitySearchService);
    changeLog = setSystemMetadata(changeLog);
    changeLog = setPreviousData(setToRestate(changeLog), changeLog);

    _updateIndicesHook.invoke(changeLog);

    // No edges added
    Mockito.verify(_mockGraphService, Mockito.times(0)).addEdge(Mockito.any());
    // Timestamp updated
    Mockito.verify(_mockGraphService, Mockito.times(1)).upsertEdge(Mockito.any());
    // No document change
    Mockito.verify(_mockEntitySearchService, Mockito.times(0))
        .upsertDocument(Mockito.any(), Mockito.any(), Mockito.any());

    /*
     * addLineage
     */
    Mockito.clearInvocations(_mockGraphService, _mockEntitySearchService);
    changeLog = setPreviousData(setToUpsert(changeLog), changeLog);
    UpstreamLineage currentLineage = addLineageEdge(createBaseLineageAspect());
    changeLog = modifyAspect(changeLog, currentLineage);

    _updateIndicesHook.invoke(changeLog);

    // New edge added
    Mockito.verify(_mockGraphService, Mockito.times(1)).addEdge(Mockito.any());
    // Update timestamp of old edge
    Mockito.verify(_mockGraphService, Mockito.times(1)).upsertEdge(Mockito.any());
    // Document update for new upstream
    Mockito.verify(_mockEntitySearchService, Mockito.times(1))
        .upsertDocument(Mockito.any(), Mockito.any(), Mockito.any());

    /*
     * restateAddLineage
     */
    Mockito.clearInvocations(_mockGraphService, _mockEntitySearchService);
    changeLog = setPreviousData(setToRestate(changeLog), changeLog);

    _updateIndicesHook.invoke(changeLog);

    // No new edges
    Mockito.verify(_mockGraphService, Mockito.times(0)).addEdge(Mockito.any());
    // Update timestamps of old edges
    Mockito.verify(_mockGraphService, Mockito.times(2)).upsertEdge(Mockito.any());
    // No document update
    Mockito.verify(_mockEntitySearchService, Mockito.times(0))
        .upsertDocument(Mockito.any(), Mockito.any(), Mockito.any());

    /*
     * noOpUpsert
     */
    Mockito.clearInvocations(_mockGraphService, _mockEntitySearchService);
    changeLog = setPreviousData(setToUpsert(changeLog), changeLog);

    _updateIndicesHook.invoke(changeLog);

    // No new edges
    Mockito.verify(_mockGraphService, Mockito.times(0)).addEdge(Mockito.any());
    // Update timestamps of old edges
    Mockito.verify(_mockGraphService, Mockito.times(2)).upsertEdge(Mockito.any());
    // No document update
    Mockito.verify(_mockEntitySearchService, Mockito.times(0))
        .upsertDocument(Mockito.any(), Mockito.any(), Mockito.any());

    /*
     * restateNoOp
     */
    Mockito.clearInvocations(_mockGraphService, _mockEntitySearchService);
    changeLog = setPreviousData(setToRestate(changeLog), changeLog);

    _updateIndicesHook.invoke(changeLog);

    // No new edges
    Mockito.verify(_mockGraphService, Mockito.times(0)).addEdge(Mockito.any());
    // Update timestamps of old edges
    Mockito.verify(_mockGraphService, Mockito.times(2)).upsertEdge(Mockito.any());
    // No document update
    Mockito.verify(_mockEntitySearchService, Mockito.times(0))
        .upsertDocument(Mockito.any(), Mockito.any(), Mockito.any());

    /*
     * systemMetadataChange
     */
    Mockito.clearInvocations(_mockGraphService, _mockEntitySearchService);
    changeLog = setPreviousData(setToUpsert(changeLog), changeLog);
    changeLog = modifySystemMetadata(changeLog);

    _updateIndicesHook.invoke(changeLog);

    // No new edges
    Mockito.verify(_mockGraphService, Mockito.times(0)).addEdge(Mockito.any());
    // Update timestamps of old edges
    Mockito.verify(_mockGraphService, Mockito.times(2)).upsertEdge(Mockito.any());
    // No document update
    Mockito.verify(_mockEntitySearchService, Mockito.times(0))
        .upsertDocument(Mockito.any(), Mockito.any(), Mockito.any());

    /*
     * restateSystemMetadataChange
     */
    Mockito.clearInvocations(_mockGraphService, _mockEntitySearchService);
    changeLog = setPreviousData(setToRestate(changeLog), changeLog);
    changeLog = modifySystemMetadata2(changeLog);

    _updateIndicesHook.invoke(changeLog);

    // No new edges
    Mockito.verify(_mockGraphService, Mockito.times(0)).addEdge(Mockito.any());
    // Update timestamps of old edges
    Mockito.verify(_mockGraphService, Mockito.times(2)).upsertEdge(Mockito.any());
    // No document update
    Mockito.verify(_mockEntitySearchService, Mockito.times(0))
        .upsertDocument(Mockito.any(), Mockito.any(), Mockito.any());

    /*
     * modifyNonSearchableField
     */
    Mockito.clearInvocations(_mockGraphService, _mockEntitySearchService);
    changeLog = setPreviousData(setToUpsert(changeLog), changeLog);
    currentLineage = modifyNonSearchableField(currentLineage);
    changeLog = modifyAspect(changeLog, currentLineage);

    _updateIndicesHook.invoke(changeLog);

    // No new edges
    Mockito.verify(_mockGraphService, Mockito.times(0)).addEdge(Mockito.any());
    // Update timestamps of old edges
    Mockito.verify(_mockGraphService, Mockito.times(2)).upsertEdge(Mockito.any());
    // No document update
    Mockito.verify(_mockEntitySearchService, Mockito.times(0))
        .upsertDocument(Mockito.any(), Mockito.any(), Mockito.any());

    /*
     * force reindexing
     */
    Mockito.clearInvocations(_mockGraphService, _mockEntitySearchService);
    changeLog = setPreviousDataToEmpty(setToRestate(changeLog));
    changeLog = setSystemMetadataWithForceIndexing(changeLog);

    _updateIndicesHook.invoke(changeLog);

    // Forced removal of all edges
    Mockito.verify(_mockGraphService, Mockito.times(1))
        .removeEdgesFromNode(Mockito.any(), Mockito.any(), Mockito.any());
    // Forced add of edges
    Mockito.verify(_mockGraphService, Mockito.times(2)).addEdge(Mockito.any());
    // Forced document update
    Mockito.verify(_mockEntitySearchService, Mockito.times(1))
        .upsertDocument(Mockito.any(), Mockito.any(), Mockito.any());
  }

  @Test
  public void testMCLUIPreProcessed() throws Exception {
    _updateIndicesService.setGraphDiffMode(true);
    _updateIndicesService.setSearchDiffMode(true);
    Urn upstreamUrn =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsDataset,PROD),foo_info)");
    Urn downstreamUrn =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD),field_foo)");

    MetadataChangeLog changeLog =
        createUpstreamLineageMCLUIPreProcessed(upstreamUrn, downstreamUrn, ChangeType.UPSERT);
    _updateIndicesHook.invoke(changeLog);
    Mockito.verifyNoInteractions(
        _mockEntitySearchService,
        _mockGraphService,
        _mockTimeseriesAspectService,
        _mockSystemMetadataService);
  }

  private EntityRegistry createMockEntityRegistry() {
    // need to mock this registry instead of using test-entity-registry.yml because inputFields does
    // not work due to a known bug
    EntityRegistry mockEntityRegistry = Mockito.mock(EntityRegistry.class);
    EntitySpec entitySpec = Mockito.mock(EntitySpec.class);
    AspectSpec aspectSpec = createMockAspectSpec(InputFields.class, InputFields.dataSchema());
    AspectSpec upstreamLineageAspectSpec =
        createMockAspectSpec(UpstreamLineage.class, UpstreamLineage.dataSchema());
    Mockito.when(mockEntityRegistry.getEntitySpec(Constants.CHART_ENTITY_NAME))
        .thenReturn(entitySpec);
    Mockito.when(mockEntityRegistry.getEntitySpec(Constants.DATASET_ENTITY_NAME))
        .thenReturn(entitySpec);
    Mockito.when(entitySpec.getAspectSpec(Constants.INPUT_FIELDS_ASPECT_NAME))
        .thenReturn(aspectSpec);
    Mockito.when(entitySpec.getAspectSpec(Constants.UPSTREAM_LINEAGE_ASPECT_NAME))
        .thenReturn(upstreamLineageAspectSpec);
    Mockito.when(aspectSpec.isTimeseries()).thenReturn(false);
    Mockito.when(aspectSpec.getName()).thenReturn(Constants.INPUT_FIELDS_ASPECT_NAME);
    Mockito.when(upstreamLineageAspectSpec.isTimeseries()).thenReturn(false);
    Mockito.when(upstreamLineageAspectSpec.getName())
        .thenReturn(Constants.UPSTREAM_LINEAGE_ASPECT_NAME);
    AspectSpec chartKeyAspectSpec = createMockAspectSpec(ChartKey.class, ChartKey.dataSchema());
    Mockito.when(entitySpec.getKeyAspectSpec()).thenReturn(chartKeyAspectSpec);
    return mockEntityRegistry;
  }

  private <T extends RecordTemplate> AspectSpec createMockAspectSpec(
      Class<T> clazz, RecordDataSchema schema) {
    AspectSpec mockSpec = Mockito.mock(AspectSpec.class);
    Mockito.when(mockSpec.getDataTemplateClass()).thenReturn((Class<RecordTemplate>) clazz);
    Mockito.when(mockSpec.getPegasusSchema()).thenReturn(schema);
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
    event.setCreated(new AuditStamp().setActor(_actorUrn).setTime(EVENT_TIME));
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
    inputField.setSchemaField(schemaField);
    inputFieldsArray.add(inputField);
    inputFields.setFields(inputFieldsArray);

    event.setAspect(GenericRecordUtils.serializeAspect(inputFields));
    event.setEntityUrn(Urn.createFromString(TEST_CHART_URN));
    event.setEntityType(Constants.CHART_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(_actorUrn).setTime(EVENT_TIME));
    return event;
  }
}
