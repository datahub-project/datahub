package com.linkedin.metadata.kafka.hook;

import static com.linkedin.metadata.graph.GraphIndexUtils.*;
import static com.linkedin.metadata.kafka.hook.EntityRegistryTestUtil.ENTITY_REGISTRY;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
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
  private static final long DEFAULT_CREATED_TIME = 1L;
  private static final long CREATED_EVENT_TIME = 123L;
  private static final long UPDATED_EVENT_TIME_1 = 234L;
  private static final long UPDATED_EVENT_TIME_2 = 345L;
  private static final Urn DATASET_URN =
      UrnUtils.getUrn(
          "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj.jaffle_shop.customers,PROD)");
  private static final DatasetUrn UPSTREAM_DATASET_1 =
      UrnUtils.toDatasetUrn("snowflake", "test", "DEV");
  private static final DatasetUrn UPSTREAM_DATASET_2 =
      UrnUtils.toDatasetUrn("snowflake", "test2", "DEV");
  private static final Urn QUERY_URN = UrnUtils.getUrn("urn:li:query:queryid");
  private static final Urn CREATED_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:creating");
  private static final Urn UPDATED_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:updating");
  private static final Urn DATAHUB_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:datahub");
  private static final String DOWNSTREAM_RELATIONSHIP_TYPE = "DownstreamOf";
  private EntityRegistry _mockRegistry;

  @BeforeMethod
  public void setupTest() {
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
            GraphIndexUtils.extractGraphEdges(entry, upstreamLineage, DATASET_URN, event, true);
        List<Edge> expectedEdgesToAdd = new ArrayList<>();
        // edges contain default created event time and created actor from system metadata
        Edge edge1 =
            new Edge(
                DATASET_URN,
                UPSTREAM_DATASET_1,
                entry.getKey().getRelationshipName(),
                CREATED_EVENT_TIME,
                CREATED_ACTOR_URN,
                UPDATED_EVENT_TIME_1,
                UPDATED_ACTOR_URN,
                Map.of("foo", "bar"),
                null,
                QUERY_URN);
        Edge edge2 =
            new Edge(
                DATASET_URN,
                UPSTREAM_DATASET_2,
                entry.getKey().getRelationshipName(),
                DEFAULT_CREATED_TIME,
                DATAHUB_ACTOR_URN,
                UPDATED_EVENT_TIME_2,
                UPDATED_ACTOR_URN,
                null);
        expectedEdgesToAdd.add(edge1);
        expectedEdgesToAdd.add(edge2);
        assertEquals(expectedEdgesToAdd.size(), edgesToAdd.size());
        Assert.assertTrue(edgesToAdd.contains(edge1));
        Assert.assertTrue(edgesToAdd.contains(edge2));
      }
    }
  }

  @Test
  public void testMergeEdges() {
    final Edge edge1 =
        new Edge(
            DATASET_URN,
            UPSTREAM_DATASET_1,
            DOWNSTREAM_RELATIONSHIP_TYPE,
            CREATED_EVENT_TIME,
            CREATED_ACTOR_URN,
            UPDATED_EVENT_TIME_1,
            UPDATED_ACTOR_URN,
            Collections.singletonMap("foo", "bar"));
    final Edge edge2 =
        new Edge(
            DATASET_URN,
            UPSTREAM_DATASET_1,
            DOWNSTREAM_RELATIONSHIP_TYPE,
            UPDATED_EVENT_TIME_2,
            UPDATED_ACTOR_URN,
            UPDATED_EVENT_TIME_2,
            UPDATED_ACTOR_URN,
            Collections.singletonMap("foo", "baz"));
    final Edge edge3 = mergeEdges(edge1, edge2);
    assertEquals(edge3.getSource(), edge1.getSource());
    assertEquals(edge3.getDestination(), edge1.getDestination());
    assertEquals(edge3.getRelationshipType(), edge1.getRelationshipType());
    assertNull(edge3.getCreatedOn());
    assertNull(edge3.getCreatedActor());
    assertEquals(edge3.getUpdatedOn(), edge2.getUpdatedOn());
    assertEquals(edge3.getUpdatedActor(), edge2.getUpdatedActor());
    assertEquals(edge3.getProperties(), edge2.getProperties());
  }

  private UpstreamLineage createUpstreamLineage() {
    UpstreamLineage upstreamLineage = new UpstreamLineage();
    UpstreamArray upstreams = new UpstreamArray();
    Upstream upstream1 = new Upstream();
    upstream1.setDataset(UPSTREAM_DATASET_1);
    upstream1.setAuditStamp(
        new AuditStamp().setActor(UPDATED_ACTOR_URN).setTime(UPDATED_EVENT_TIME_1));
    upstream1.setCreated(new AuditStamp().setActor(CREATED_ACTOR_URN).setTime(CREATED_EVENT_TIME));
    upstream1.setProperties(new StringMap(Map.of("foo", "bar")));
    upstream1.setQuery(QUERY_URN);
    upstream1.setType(DatasetLineageType.TRANSFORMED);

    Upstream upstream2 = new Upstream();
    upstream2.setDataset(UPSTREAM_DATASET_2);
    upstream2.setAuditStamp(
        new AuditStamp().setActor(UPDATED_ACTOR_URN).setTime(UPDATED_EVENT_TIME_1));
    upstream2.setType(DatasetLineageType.COPY);
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
    event.setEntityUrn(DATASET_URN);

    SystemMetadata systemMetadata = new SystemMetadata();
    systemMetadata.setLastObserved(CREATED_EVENT_TIME);
    event.setSystemMetadata(systemMetadata);
    event.setCreated(new AuditStamp().setActor(DATAHUB_ACTOR_URN).setTime(DEFAULT_CREATED_TIME));

    return event;
  }
}
