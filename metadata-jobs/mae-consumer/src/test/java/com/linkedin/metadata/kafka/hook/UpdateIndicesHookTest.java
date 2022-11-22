package com.linkedin.metadata.kafka.hook;

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
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.dataset.FineGrainedLineage;
import com.linkedin.dataset.FineGrainedLineageArray;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.Edge;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.key.ChartKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.schema.SchemaField;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.search.utils.QueryUtils.newRelationshipFilter;

public class UpdateIndicesHookTest {
//  going to want a test where we have an upstreamLineage aspect with finegrained, check that we call _graphService.addEdge for each edge
//  as well as _graphService.removeEdgesFromNode for each field and their relationships

  private static final long EVENT_TIME = 123L;
  private static final String TEST_DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)";
  private static final String TEST_CHART_URN = "urn:li:chart:(looker,dashboard_elements.1)";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:test";
  private static final String DOWNSTREAM_OF = "DownstreamOf";
  private UpdateIndicesHook _updateIndicesHook;
  private GraphService _mockGraphService;
  private EntitySearchService _mockEntitySearchService;
  private TimeseriesAspectService _mockTimeseriesAspectService;
  private SystemMetadataService _mockSystemMetadataService;
  private SearchDocumentTransformer _mockSearchDocumentTransformer;
  private Urn _actorUrn;

  @BeforeMethod
  public void setupTest() {
    _actorUrn = UrnUtils.getUrn(TEST_ACTOR_URN);
    EntityRegistry registry = new ConfigEntityRegistry(
        UpdateIndicesHookTest.class.getClassLoader().getResourceAsStream("test-entity-registry.yml"));
    _mockGraphService = Mockito.mock(GraphService.class);
    _mockEntitySearchService = Mockito.mock(EntitySearchService.class);
    _mockTimeseriesAspectService = Mockito.mock(TimeseriesAspectService.class);
    _mockSystemMetadataService = Mockito.mock(SystemMetadataService.class);
    _mockSearchDocumentTransformer = Mockito.mock(SearchDocumentTransformer.class);
    _updateIndicesHook = new UpdateIndicesHook(
        _mockGraphService,
        _mockEntitySearchService,
        _mockTimeseriesAspectService,
        _mockSystemMetadataService,
        registry,
        _mockSearchDocumentTransformer
    );
  }

  @Test
  public void testFineGrainedLineageEdgesAreAdded() throws Exception {
    Urn upstreamUrn = UrnUtils.getUrn("urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsDataset,PROD),foo_info)");
    Urn downstreamUrn = UrnUtils.getUrn("urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD),field_foo)");
    MetadataChangeLog event = createUpstreamLineageMCL(upstreamUrn, downstreamUrn);
    _updateIndicesHook.invoke(event);

    Edge edge = new Edge(downstreamUrn, upstreamUrn, DOWNSTREAM_OF);
    Mockito.verify(_mockGraphService, Mockito.times(1)).addEdge(Mockito.eq(edge));
    Mockito.verify(_mockGraphService, Mockito.times(1)).removeEdgesFromNode(
        Mockito.eq(downstreamUrn),
        Mockito.eq(new ArrayList<>(Collections.singleton(DOWNSTREAM_OF))),
        Mockito.eq(newRelationshipFilter(new Filter().setOr(new ConjunctiveCriterionArray()), RelationshipDirection.OUTGOING))
    );
  }

  @Test
  public void testInputFieldsEdgesAreAdded() throws Exception {
    Urn upstreamUrn = UrnUtils.getUrn("urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:looker,thelook.explore.order_items,PROD),users.count)");
    String downstreamFieldPath = "users.count";
    MetadataChangeLog event = createInputFieldsMCL(upstreamUrn, downstreamFieldPath);
    EntityRegistry mockEntityRegistry = createMockEntityRegistry();
    _updateIndicesHook = new UpdateIndicesHook(
        _mockGraphService,
        _mockEntitySearchService,
        _mockTimeseriesAspectService,
        _mockSystemMetadataService,
        mockEntityRegistry,
        _mockSearchDocumentTransformer
    );

    _updateIndicesHook.invoke(event);

    Urn downstreamUrn = UrnUtils.getUrn(String.format("urn:li:schemaField:(%s,%s)", TEST_CHART_URN, downstreamFieldPath));

    Edge edge = new Edge(downstreamUrn, upstreamUrn, DOWNSTREAM_OF);
    Mockito.verify(_mockGraphService, Mockito.times(1)).addEdge(Mockito.eq(edge));
    Mockito.verify(_mockGraphService, Mockito.times(1)).removeEdgesFromNode(
        Mockito.eq(downstreamUrn),
        Mockito.eq(new ArrayList<>(Collections.singleton(DOWNSTREAM_OF))),
        Mockito.eq(newRelationshipFilter(new Filter().setOr(new ConjunctiveCriterionArray()), RelationshipDirection.OUTGOING))
    );
  }

  private EntityRegistry createMockEntityRegistry() {
    // need to mock this registry instead of using test-entity-registry.yml because inputFields does not work due to a known bug
    EntityRegistry mockEntityRegistry = Mockito.mock(EntityRegistry.class);
    EntitySpec entitySpec = Mockito.mock(EntitySpec.class);
    AspectSpec aspectSpec = createMockAspectSpec(InputFields.class, InputFields.dataSchema());
    Mockito.when(mockEntityRegistry.getEntitySpec(Constants.CHART_ENTITY_NAME)).thenReturn(entitySpec);
    Mockito.when(entitySpec.getAspectSpec(Constants.INPUT_FIELDS_ASPECT_NAME)).thenReturn(aspectSpec);
    Mockito.when(aspectSpec.isTimeseries()).thenReturn(false);
    Mockito.when(aspectSpec.getName()).thenReturn(Constants.INPUT_FIELDS_ASPECT_NAME);
    AspectSpec chartKeyAspectSpec = createMockAspectSpec(ChartKey.class, ChartKey.dataSchema());
    Mockito.when(entitySpec.getKeyAspectSpec()).thenReturn(chartKeyAspectSpec);
    return mockEntityRegistry;
  }

  private <T extends RecordTemplate> AspectSpec createMockAspectSpec(Class<T> clazz, RecordDataSchema schema) {
    AspectSpec mockSpec = Mockito.mock(AspectSpec.class);
    Mockito.when(mockSpec.getDataTemplateClass()).thenReturn((Class<RecordTemplate>) clazz);
    Mockito.when(mockSpec.getPegasusSchema()).thenReturn(schema);
    return mockSpec;
  }

  private MetadataChangeLog createUpstreamLineageMCL(Urn upstreamUrn, Urn downstreamUrn) throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(Constants.DATASET_ENTITY_NAME);
    event.setAspectName(Constants.UPSTREAM_LINEAGE_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);

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
    upstream.setDataset(DatasetUrn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsDataset,PROD)"));
    upstreamArray.add(upstream);
    upstreamLineage.setUpstreams(upstreamArray);

    event.setAspect(GenericRecordUtils.serializeAspect(upstreamLineage));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(_actorUrn).setTime(EVENT_TIME));
    return event;
  }

  private MetadataChangeLog createInputFieldsMCL(Urn upstreamUrn, String downstreamFieldPath) throws Exception {
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
