package com.linkedin.metadata.kafka.hook;

import static com.linkedin.metadata.graph.GraphIndexUtils.*;
import static com.linkedin.metadata.kafka.hook.EntityRegistryTestUtil.ENTITY_REGISTRY;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.graph.GraphIndexUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.extractor.FieldExtractor;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GraphIndexUtilsTest {

  private static final String UPSTREAM_RELATIONSHIP_PATH = "/upstreams/*/dataset";
  private static final long CREATED_EVENT_TIME = 123L;
  private static final long UPDATED_EVENT_TIME_1 = 234L;
  private static final long UPDATED_EVENT_TIME_2 = 345L;
  private Urn _datasetUrn;
  private DatasetUrn _upstreamDataset1;
  private DatasetUrn _upstreamDataset2;
  private static final String CREATED_ACTOR_URN = "urn:li:corpuser:creating";
  private static final String UPDATED_ACTOR_URN = "urn:li:corpuser:updating";
  private static final String DOWNSTREAM_RELATIONSHIP_TYPE = "DownstreamOf";
  private EntityRegistry _mockRegistry;
  private Urn _createdActorUrn;
  private Urn _updatedActorUrn;

  @BeforeMethod
  public void setupTest() {
    _createdActorUrn = UrnUtils.getUrn(CREATED_ACTOR_URN);
    _updatedActorUrn = UrnUtils.getUrn(UPDATED_ACTOR_URN);
    _datasetUrn =
        UrnUtils.getUrn(
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj.jaffle_shop.customers,PROD)");
    _upstreamDataset1 = UrnUtils.toDatasetUrn("snowflake", "test", "DEV");
    _upstreamDataset2 = UrnUtils.toDatasetUrn("snowflake", "test2", "DEV");
    _mockRegistry = ENTITY_REGISTRY;
  }

  @Test
  public void testExtractGraphEdgesDefault() {
    UpstreamLineage upstreamLineage = createUpstreamLineage();
    MetadataChangeLog event = createMCL(upstreamLineage);

    EntitySpec entitySpec = _mockRegistry.getEntitySpec(event.getEntityType());
    AspectSpec aspectSpec = entitySpec.getAspectSpec(event.getAspectName());

    Map<RelationshipFieldSpec, List<Object>> extractedFields =
        FieldExtractor.extractFields(upstreamLineage, aspectSpec.getRelationshipFieldSpecs());

    for (Map.Entry<RelationshipFieldSpec, List<Object>> entry : extractedFields.entrySet()) {
      // check specifically for the upstreams relationship entry
      if (entry.getKey().getPath().toString().equals(UPSTREAM_RELATIONSHIP_PATH)) {
        List<Edge> edgesToAdd =
            GraphIndexUtils.extractGraphEdges(entry, upstreamLineage, _datasetUrn, event, true);
        List<Edge> expectedEdgesToAdd = new ArrayList<>();
        // edges contain default created event time and created actor from system metadata
        Edge edge1 =
            new Edge(
                _datasetUrn,
                _upstreamDataset1,
                entry.getKey().getRelationshipName(),
                CREATED_EVENT_TIME,
                _createdActorUrn,
                UPDATED_EVENT_TIME_1,
                _updatedActorUrn,
                null);
        Edge edge2 =
            new Edge(
                _datasetUrn,
                _upstreamDataset2,
                entry.getKey().getRelationshipName(),
                CREATED_EVENT_TIME,
                _createdActorUrn,
                UPDATED_EVENT_TIME_2,
                _updatedActorUrn,
                null);
        expectedEdgesToAdd.add(edge1);
        expectedEdgesToAdd.add(edge2);
        assertEquals(expectedEdgesToAdd.size(), edgesToAdd.size());
        Assert.assertTrue(edgesToAdd.containsAll(expectedEdgesToAdd));
        Assert.assertTrue(expectedEdgesToAdd.containsAll(edgesToAdd));
      }
    }
  }

  @Test
  public void testMergeEdges() {
    final Edge edge1 =
        new Edge(
            _datasetUrn,
            _upstreamDataset1,
            DOWNSTREAM_RELATIONSHIP_TYPE,
            CREATED_EVENT_TIME,
            _createdActorUrn,
            UPDATED_EVENT_TIME_1,
            _updatedActorUrn,
            Collections.singletonMap("foo", "bar"));
    final Edge edge2 =
        new Edge(
            _datasetUrn,
            _upstreamDataset1,
            DOWNSTREAM_RELATIONSHIP_TYPE,
            UPDATED_EVENT_TIME_2,
            _updatedActorUrn,
            UPDATED_EVENT_TIME_2,
            _updatedActorUrn,
            Collections.singletonMap("foo", "baz"));
    final Edge edge3 = mergeEdges(edge1, edge2);
    assertEquals(edge3.getSource(), edge1.getSource());
    assertEquals(edge3.getDestination(), edge1.getDestination());
    assertEquals(edge3.getRelationshipType(), edge1.getRelationshipType());
    assertEquals(edge3.getCreatedOn(), null);
    assertEquals(edge3.getCreatedActor(), null);
    assertEquals(edge3.getUpdatedOn(), edge2.getUpdatedOn());
    assertEquals(edge3.getUpdatedActor(), edge2.getUpdatedActor());
    assertEquals(edge3.getProperties(), edge2.getProperties());
  }

  private UpstreamLineage createUpstreamLineage() {
    UpstreamLineage upstreamLineage = new UpstreamLineage();
    UpstreamArray upstreams = new UpstreamArray();
    Upstream upstream1 = new Upstream();
    upstream1.setDataset(_upstreamDataset1);
    upstream1.setAuditStamp(
        new AuditStamp().setActor(_updatedActorUrn).setTime(UPDATED_EVENT_TIME_1));
    upstream1.setType(DatasetLineageType.TRANSFORMED);
    Upstream upstream2 = new Upstream();
    upstream2.setDataset(_upstreamDataset2);
    upstream2.setAuditStamp(
        new AuditStamp().setActor(_updatedActorUrn).setTime(UPDATED_EVENT_TIME_1));
    upstream2.setType(DatasetLineageType.TRANSFORMED);
    upstreams.add(upstream1);
    upstreams.add(upstream2);
    upstreamLineage.setUpstreams(upstreams);

    return upstreamLineage;
  }

  private MetadataChangeLog createMCL(RecordTemplate aspect) {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(Constants.DATASET_ENTITY_NAME);
    event.setAspectName(Constants.UPSTREAM_LINEAGE_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);

    event.setAspect(GenericRecordUtils.serializeAspect(aspect));
    event.setEntityUrn(_datasetUrn);

    SystemMetadata systemMetadata = new SystemMetadata();
    systemMetadata.setLastObserved(CREATED_EVENT_TIME);
    event.setSystemMetadata(systemMetadata);
    event.setCreated(new AuditStamp().setActor(_createdActorUrn).setTime(CREATED_EVENT_TIME));

    return event;
  }
}
