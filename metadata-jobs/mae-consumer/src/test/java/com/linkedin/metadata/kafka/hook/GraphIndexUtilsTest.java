package com.linkedin.metadata.kafka.hook;

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
import com.linkedin.metadata.graph.Edge;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.extractor.FieldExtractor;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GraphIndexUtilsTest {

  private static final String UPSTREAM_RELATIONSHIP_PATH = "/upstreams/*/dataset";
  private static final long CREATED_EVENT_TIME = 123L;
  private static final long UPDATED_EVENT_TIME = 234L;
  private Urn _datasetUrn;
  private DatasetUrn _upstreamDataset1;
  private DatasetUrn _upstreamDataset2;
  private static final String CREATED_ACTOR_URN = "urn:li:corpuser:creating";
  private static final String UPDATED_ACTOR_URN = "urn:li:corpuser:updating";
  private EntityRegistry _mockRegistry;
  private Urn _createdActorUrn;
  private Urn _updatedActorUrn;

  @BeforeMethod
  public void setupTest() {
    _createdActorUrn = UrnUtils.getUrn(CREATED_ACTOR_URN);
    _updatedActorUrn = UrnUtils.getUrn(UPDATED_ACTOR_URN);
    _datasetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj.jaffle_shop.customers,PROD)");
    _upstreamDataset1 = UrnUtils.toDatasetUrn("snowflake", "test", "DEV");
    _upstreamDataset2 = UrnUtils.toDatasetUrn("snowflake", "test2", "DEV");
    _mockRegistry = new ConfigEntityRegistry(
        UpdateIndicesHookTest.class.getClassLoader().getResourceAsStream("test-entity-registry.yml"));
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
        List<Edge> edgesToAdd = GraphIndexUtils.extractGraphEdges(entry, upstreamLineage, _datasetUrn, event);
        List<Edge> expectedEdgesToAdd = new ArrayList<>();
        // edges contain default created event time and created actor from system metadata
        Edge edge1 = new Edge(_datasetUrn, _upstreamDataset1, entry.getKey().getRelationshipName(), CREATED_EVENT_TIME, _createdActorUrn, null, null);
        Edge edge2 = new Edge(_datasetUrn, _upstreamDataset2, entry.getKey().getRelationshipName(), CREATED_EVENT_TIME, _createdActorUrn, null, null);
        expectedEdgesToAdd.add(edge1);
        expectedEdgesToAdd.add(edge2);
        Assert.assertEquals(expectedEdgesToAdd.size(), edgesToAdd.size());
        Assert.assertTrue(edgesToAdd.containsAll(expectedEdgesToAdd));
        Assert.assertTrue(expectedEdgesToAdd.containsAll(edgesToAdd));
      }
    }
  }

  private UpstreamLineage createUpstreamLineage() {
    UpstreamLineage upstreamLineage = new UpstreamLineage();
    UpstreamArray upstreams = new UpstreamArray();
    Upstream upstream1 = new Upstream();
    upstream1.setDataset(_upstreamDataset1);
    upstream1.setAuditStamp(new AuditStamp().setActor(_updatedActorUrn).setTime(UPDATED_EVENT_TIME));
    upstream1.setType(DatasetLineageType.TRANSFORMED);
    Upstream upstream2 = new Upstream();
    upstream2.setDataset(_upstreamDataset2);
    upstream2.setAuditStamp(new AuditStamp().setActor(_updatedActorUrn).setTime(UPDATED_EVENT_TIME));
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
