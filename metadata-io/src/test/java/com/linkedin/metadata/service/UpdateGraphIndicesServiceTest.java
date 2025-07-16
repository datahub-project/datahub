package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.CONTAINER_ENTITY_NAME;
import static com.linkedin.metadata.search.utils.QueryUtils.createRelationshipFilter;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.container.Container;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.models.graph.EdgeUrnType;
import com.linkedin.metadata.entity.TestEntityRegistry;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.elastic.ESGraphQueryDAO;
import com.linkedin.metadata.graph.elastic.ESGraphWriteDAO;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.mockito.ArgumentCaptor;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.script.Script;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class UpdateGraphIndicesServiceTest {
  private static final Urn TEST_URN =
      UrnUtils.getUrn(
          "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.pet_profiles,PROD)");
  private static final OperationContext TEST_OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private UpdateGraphIndicesService test;
  private ESBulkProcessor mockESBulkProcessor;
  private ESGraphWriteDAO mockWriteDAO;
  private ESGraphQueryDAO mockReadDAO;

  @BeforeTest
  public void beforeTest() {
    EntityRegistry entityRegistry = new TestEntityRegistry();
    mockESBulkProcessor = mock(ESBulkProcessor.class);
    mockWriteDAO = mock(ESGraphWriteDAO.class);
    mockReadDAO = mock(ESGraphQueryDAO.class);

    test =
        new UpdateGraphIndicesService(
            new ElasticSearchGraphService(
                new LineageRegistry(entityRegistry),
                mockESBulkProcessor,
                IndexConventionImpl.noPrefix("md5"),
                mockWriteDAO,
                mockReadDAO,
                mock(ESIndexBuilder.class),
                "md5"));
  }

  @BeforeMethod
  public void beforeMethod() {
    reset(mockESBulkProcessor, mockWriteDAO, mockReadDAO);
  }

  @Test
  public void testStatusDeleteEvent() {
    test.handleChangeEvent(
        TEST_OP_CONTEXT,
        new MetadataChangeLog()
            .setChangeType(ChangeType.DELETE)
            .setEntityType("dataset")
            .setEntityUrn(TEST_URN)
            .setAspectName(Constants.STATUS_ASPECT_NAME));

    ArgumentCaptor<Script> scriptCaptor = ArgumentCaptor.forClass(Script.class);
    verify(mockWriteDAO, times(EdgeUrnType.values().length))
        .updateByQuery(scriptCaptor.capture(), any(QueryBuilder.class));

    scriptCaptor
        .getAllValues()
        .forEach(
            script ->
                assertEquals(
                    script.getParams().get("newValue").toString(),
                    "false",
                    "Status delete implies setting the removed status to false"));
  }

  @Test
  public void testStatusUpdateEvent() {
    GenericAspect removedTrue = GenericRecordUtils.serializeAspect(new Status().setRemoved(true));
    GenericAspect removedFalse = GenericRecordUtils.serializeAspect(new Status().setRemoved(false));

    // 1. no previous version
    test.handleChangeEvent(
        TEST_OP_CONTEXT,
        new MetadataChangeLog()
            .setChangeType(ChangeType.UPSERT)
            .setEntityType("dataset")
            .setEntityUrn(TEST_URN)
            .setAspectName(Constants.STATUS_ASPECT_NAME)
            .setAspect(removedTrue));
    ArgumentCaptor<Script> scriptCaptor = ArgumentCaptor.forClass(Script.class);
    verify(mockWriteDAO, times(EdgeUrnType.values().length))
        .updateByQuery(scriptCaptor.capture(), any(QueryBuilder.class));
    scriptCaptor
        .getAllValues()
        .forEach(script -> assertEquals(script.getParams().get("newValue").toString(), "true"));

    // 2. differing previous version
    reset(mockWriteDAO);
    scriptCaptor = ArgumentCaptor.forClass(Script.class);
    test.handleChangeEvent(
        TEST_OP_CONTEXT,
        new MetadataChangeLog()
            .setChangeType(ChangeType.UPSERT)
            .setEntityType("dataset")
            .setEntityUrn(TEST_URN)
            .setAspectName(Constants.STATUS_ASPECT_NAME)
            .setPreviousAspectValue(removedTrue)
            .setAspect(removedFalse));
    verify(mockWriteDAO, times(EdgeUrnType.values().length))
        .updateByQuery(scriptCaptor.capture(), any(QueryBuilder.class));
    scriptCaptor
        .getAllValues()
        .forEach(script -> assertEquals(script.getParams().get("newValue").toString(), "false"));

    // 3. RESTATE with no difference
    reset(mockWriteDAO);
    scriptCaptor = ArgumentCaptor.forClass(Script.class);
    test.handleChangeEvent(
        TEST_OP_CONTEXT,
        new MetadataChangeLog()
            .setChangeType(ChangeType.RESTATE)
            .setEntityType("dataset")
            .setEntityUrn(TEST_URN)
            .setAspectName(Constants.STATUS_ASPECT_NAME)
            .setPreviousAspectValue(removedTrue)
            .setAspect(removedTrue));
    verify(mockWriteDAO, times(EdgeUrnType.values().length))
        .updateByQuery(scriptCaptor.capture(), any(QueryBuilder.class));
    scriptCaptor
        .getAllValues()
        .forEach(script -> assertEquals(script.getParams().get("newValue").toString(), "true"));
  }

  @Test
  public void testStatusNoOpEvent() {
    // 1. non status aspect
    test.handleChangeEvent(
        TEST_OP_CONTEXT,
        new MetadataChangeLog()
            .setChangeType(ChangeType.UPSERT)
            .setEntityType("dataset")
            .setEntityUrn(TEST_URN)
            .setAspectName(Constants.DATASET_PROPERTIES_ASPECT_NAME)
            .setAspect(GenericRecordUtils.serializeAspect(new DatasetProperties())));

    // 2. no change in status
    GenericAspect statusAspect = GenericRecordUtils.serializeAspect(new Status().setRemoved(true));
    test.handleChangeEvent(
        TEST_OP_CONTEXT,
        new MetadataChangeLog()
            .setChangeType(ChangeType.UPSERT)
            .setEntityType("dataset")
            .setEntityUrn(TEST_URN)
            .setAspectName(Constants.STATUS_ASPECT_NAME)
            .setPreviousAspectValue(statusAspect)
            .setAspect(statusAspect));

    verifyNoInteractions(mockWriteDAO);
  }

  @Test
  public void testMissingAspectGraphDelete() {
    // Test deleting a null aspect
    test.handleChangeEvent(
        TEST_OP_CONTEXT,
        new MetadataChangeLog()
            .setChangeType(ChangeType.DELETE)
            .setEntityType(TEST_URN.getEntityType())
            .setEntityUrn(TEST_URN)
            .setAspectName(Constants.CONTAINER_ASPECT_NAME));

    // For missing aspects, verify no writes
    verifyNoInteractions(mockWriteDAO);
  }

  @Test
  public void testNodeGraphDelete() {
    Urn containerUrn = UrnUtils.getUrn("urn:li:container:foo");

    // Test deleting container entity
    test.handleChangeEvent(
        TEST_OP_CONTEXT,
        new MetadataChangeLog()
            .setChangeType(ChangeType.DELETE)
            .setEntityType(CONTAINER_ENTITY_NAME)
            .setEntityUrn(containerUrn)
            .setAspectName(Constants.CONTAINER_KEY_ASPECT_NAME));

    // Delete all outgoing edges of this entity
    verify(mockWriteDAO, times(1))
        .deleteByQuery(
            eq(TEST_OP_CONTEXT),
            eq(
                GraphFilters.from(
                    createUrnFilter(containerUrn),
                    Set.of(),
                    new RelationshipFilter().setDirection(RelationshipDirection.OUTGOING))));

    // Delete all incoming edges of this entity
    verify(mockWriteDAO, times(1))
        .deleteByQuery(
            eq(TEST_OP_CONTEXT),
            eq(
                GraphFilters.from(
                    createUrnFilter(containerUrn),
                    Set.of(),
                    new RelationshipFilter().setDirection(RelationshipDirection.INCOMING))));

    // Delete all edges where this entity is a lifecycle owner
    verify(mockWriteDAO, times(1))
        .deleteByQuery(eq(TEST_OP_CONTEXT), eq(GraphFilters.ALL), eq(containerUrn.toString()));
  }

  @Test
  public void testContainerDelete() {
    Urn containerUrn = UrnUtils.getUrn("urn:li:container:foo");

    // Test deleting a container aspect
    test.handleChangeEvent(
        TEST_OP_CONTEXT,
        new MetadataChangeLog()
            .setChangeType(ChangeType.DELETE)
            .setEntityType(TEST_URN.getEntityType())
            .setEntityUrn(TEST_URN)
            .setAspectName(Constants.CONTAINER_ASPECT_NAME)
            .setPreviousAspectValue(
                GenericRecordUtils.serializeAspect(new Container().setContainer(containerUrn))));

    // For container aspects, verify that only edges are removed in both cases
    verify(mockWriteDAO, times(1))
        .deleteByQuery(
            eq(TEST_OP_CONTEXT),
            eq(
                GraphFilters.from(
                    createUrnFilter(TEST_URN),
                    Set.of("IsPartOf"),
                    createRelationshipFilter(
                        new Filter().setOr(new ConjunctiveCriterionArray()),
                        RelationshipDirection.OUTGOING))));
  }

  @Test
  public void testUpdateGraphServiceDiffWithSubtractiveDifference() throws URISyntaxException {
    // Enable diff mode
    test.setGraphDiffMode(true);

    // Create upstream lineage with 2 upstreams initially
    Urn upstream1 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,upstream1,PROD)");
    Urn upstream2 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,upstream2,PROD)");

    UpstreamArray upstreamArray = new UpstreamArray();
    upstreamArray.add(createUpstream(upstream1));
    upstreamArray.add(createUpstream(upstream2));

    UpstreamLineage oldUpstreamLineage = new UpstreamLineage().setUpstreams(upstreamArray);
    GenericAspect oldAspect = GenericRecordUtils.serializeAspect(oldUpstreamLineage);

    // New upstream lineage with only 1 upstream (removing upstream2)
    UpstreamArray newUpstreamArray = new UpstreamArray();
    newUpstreamArray.add(createUpstream(upstream1));

    UpstreamLineage newUpstreamLineage = new UpstreamLineage().setUpstreams(newUpstreamArray);
    GenericAspect newAspect = GenericRecordUtils.serializeAspect(newUpstreamLineage);

    test.handleChangeEvent(
        TEST_OP_CONTEXT,
        new MetadataChangeLog()
            .setChangeType(ChangeType.UPSERT)
            .setEntityType("dataset")
            .setEntityUrn(TEST_URN)
            .setAspectName(Constants.UPSTREAM_LINEAGE_ASPECT_NAME)
            .setPreviousAspectValue(oldAspect)
            .setAspect(newAspect));

    // Verify that deleteDocument was called for the removed edge
    ArgumentCaptor<String> docIdCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockWriteDAO, times(1)).deleteDocument(docIdCaptor.capture());

    String docId = docIdCaptor.getValue();
    // The doc ID should contain hash of the edge including source and destination
    assertNotNull(docId);
  }

  @Test
  public void testUpdateGraphServiceDiffWithAdditiveDifference() throws URISyntaxException {
    // Enable diff mode
    test.setGraphDiffMode(true);

    Urn upstream1 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,upstream1,PROD)");
    Urn upstream2 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,upstream2,PROD)");

    // Old upstream lineage with 1 upstream
    UpstreamArray oldUpstreamArray = new UpstreamArray();
    oldUpstreamArray.add(createUpstream(upstream1));

    UpstreamLineage oldUpstreamLineage = new UpstreamLineage().setUpstreams(oldUpstreamArray);
    GenericAspect oldAspect = GenericRecordUtils.serializeAspect(oldUpstreamLineage);

    // New upstream lineage with 2 upstreams (adding upstream2)
    UpstreamArray newUpstreamArray = new UpstreamArray();
    newUpstreamArray.add(createUpstream(upstream1));
    newUpstreamArray.add(createUpstream(upstream2));

    UpstreamLineage newUpstreamLineage = new UpstreamLineage().setUpstreams(newUpstreamArray);
    GenericAspect newAspect = GenericRecordUtils.serializeAspect(newUpstreamLineage);

    test.handleChangeEvent(
        TEST_OP_CONTEXT,
        new MetadataChangeLog()
            .setChangeType(ChangeType.UPSERT)
            .setEntityType("dataset")
            .setEntityUrn(TEST_URN)
            .setAspectName(Constants.UPSTREAM_LINEAGE_ASPECT_NAME)
            .setPreviousAspectValue(oldAspect)
            .setAspect(newAspect));

    // Verify that upsertDocument was called for the new edge
    ArgumentCaptor<String> docIdCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> documentCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockWriteDAO, times(2)).upsertDocument(docIdCaptor.capture(), documentCaptor.capture());

    String docId = docIdCaptor.getValue();
    assertNotNull(docId);
  }

  @Test
  public void testUpdateGraphServiceDiffWithMergedEdges() throws URISyntaxException {
    // Enable diff mode
    test.setGraphDiffMode(true);

    Urn upstream1 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,upstream1,PROD)");

    // Create old upstream with one audit stamp
    AuditStamp oldAuditStamp = new AuditStamp().setActor(new CorpuserUrn("user1")).setTime(1000L);

    Upstream oldUpstream =
        new Upstream()
            .setDataset(DatasetUrn.createFromUrn(upstream1))
            .setType(DatasetLineageType.TRANSFORMED)
            .setAuditStamp(oldAuditStamp);

    UpstreamArray oldUpstreamArray = new UpstreamArray();
    oldUpstreamArray.add(oldUpstream);

    UpstreamLineage oldUpstreamLineage = new UpstreamLineage().setUpstreams(oldUpstreamArray);
    GenericAspect oldAspect = GenericRecordUtils.serializeAspect(oldUpstreamLineage);

    // Create new upstream with updated audit stamp (same upstream, different properties)
    AuditStamp newAuditStamp = new AuditStamp().setActor(new CorpuserUrn("user2")).setTime(2000L);

    Upstream newUpstream =
        new Upstream()
            .setDataset(DatasetUrn.createFromUrn(upstream1))
            .setType(DatasetLineageType.TRANSFORMED)
            .setAuditStamp(newAuditStamp);

    UpstreamArray newUpstreamArray = new UpstreamArray();
    newUpstreamArray.add(newUpstream);

    UpstreamLineage newUpstreamLineage = new UpstreamLineage().setUpstreams(newUpstreamArray);
    GenericAspect newAspect = GenericRecordUtils.serializeAspect(newUpstreamLineage);

    test.handleChangeEvent(
        TEST_OP_CONTEXT,
        new MetadataChangeLog()
            .setChangeType(ChangeType.UPSERT)
            .setEntityType("dataset")
            .setEntityUrn(TEST_URN)
            .setAspectName(Constants.UPSTREAM_LINEAGE_ASPECT_NAME)
            .setPreviousAspectValue(oldAspect)
            .setAspect(newAspect));

    // Verify that upsertDocument was called for the updated edge
    ArgumentCaptor<String> docIdCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> documentCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockWriteDAO, times(1)).upsertDocument(docIdCaptor.capture(), documentCaptor.capture());

    String document = documentCaptor.getValue();
    // The updated document should contain the new audit stamp info
    assertTrue(document.contains("user2"));
    assertTrue(document.contains("2000"));
  }

  @Test
  public void testUpdateGraphServiceDiffWithAllThreeConditions() throws URISyntaxException {
    // Enable diff mode
    test.setGraphDiffMode(true);

    Urn upstream1 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,upstream1,PROD)");
    Urn upstream2 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,upstream2,PROD)");
    Urn upstream3 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,upstream3,PROD)");

    // Old: upstream1 (will be updated), upstream2 (will be removed)
    UpstreamArray oldUpstreamArray = new UpstreamArray();
    oldUpstreamArray.add(createUpstream(upstream1));
    oldUpstreamArray.add(createUpstream(upstream2));

    UpstreamLineage oldUpstreamLineage = new UpstreamLineage().setUpstreams(oldUpstreamArray);
    GenericAspect oldAspect = GenericRecordUtils.serializeAspect(oldUpstreamLineage);

    // New: upstream1 (updated with new timestamp), upstream3 (new)
    AuditStamp newAuditStamp =
        new AuditStamp().setActor(new CorpuserUrn("updatedUser")).setTime(3000L);

    Upstream updatedUpstream =
        new Upstream()
            .setDataset(DatasetUrn.createFromUrn(upstream1))
            .setType(DatasetLineageType.TRANSFORMED)
            .setAuditStamp(newAuditStamp);

    UpstreamArray newUpstreamArray = new UpstreamArray();
    newUpstreamArray.add(updatedUpstream);
    newUpstreamArray.add(createUpstream(upstream3));

    UpstreamLineage newUpstreamLineage = new UpstreamLineage().setUpstreams(newUpstreamArray);
    GenericAspect newAspect = GenericRecordUtils.serializeAspect(newUpstreamLineage);

    test.handleChangeEvent(
        TEST_OP_CONTEXT,
        new MetadataChangeLog()
            .setChangeType(ChangeType.UPSERT)
            .setEntityType("dataset")
            .setEntityUrn(TEST_URN)
            .setAspectName(Constants.UPSTREAM_LINEAGE_ASPECT_NAME)
            .setPreviousAspectValue(oldAspect)
            .setAspect(newAspect));

    // Verify all three operations occurred
    // 1 delete for upstream2 removal
    verify(mockWriteDAO, times(1)).deleteDocument(any(String.class));

    // 2 upserts: one for upstream1 update, one for upstream3 addition
    ArgumentCaptor<String> documentCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockWriteDAO, times(2)).upsertDocument(any(String.class), documentCaptor.capture());

    List<String> documents = documentCaptor.getAllValues();
    // Verify that we have documents for both the update and the addition
    assertTrue(documents.stream().anyMatch(doc -> doc.contains("updatedUser")));
    assertTrue(documents.stream().anyMatch(doc -> doc.contains(upstream3.toString())));
  }

  // Helper method
  private Upstream createUpstream(Urn datasetUrn) throws URISyntaxException {
    return new Upstream()
        .setDataset(DatasetUrn.createFromUrn(datasetUrn))
        .setType(DatasetLineageType.TRANSFORMED)
        .setAuditStamp(
            new AuditStamp()
                .setActor(new CorpuserUrn("testUser"))
                .setTime(System.currentTimeMillis()));
  }

  private static Filter createUrnFilter(@Nonnull final Urn urn) {
    Filter filter = new Filter();
    CriterionArray criterionArray = new CriterionArray();
    Criterion criterion = buildCriterion("urn", Condition.EQUAL, urn.toString());
    criterionArray.add(criterion);
    filter.setOr(
        new ConjunctiveCriterionArray(
            ImmutableList.of(new ConjunctiveCriterion().setAnd(criterionArray))));

    return filter;
  }
}
