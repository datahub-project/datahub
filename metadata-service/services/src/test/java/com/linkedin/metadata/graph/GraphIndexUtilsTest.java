package com.linkedin.metadata.graph;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
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
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.annotation.RelationshipAnnotation;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GraphIndexUtilsTest {

  private static final String DATASET_URN_STRING =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table,PROD)";
  private static final String UPSTREAM_URN_STRING =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,upstream.table,PROD)";
  private static final String ACTOR_URN_STRING = "urn:li:corpuser:testuser";
  private static final String RELATIONSHIP_NAME = "DownstreamOf";

  private Urn datasetUrn;
  private Urn upstreamUrn;
  private Urn actorUrn;

  @BeforeMethod
  public void setUp() {
    datasetUrn = UrnUtils.getUrn(DATASET_URN_STRING);
    upstreamUrn = UrnUtils.getUrn(UPSTREAM_URN_STRING);
    actorUrn = UrnUtils.getUrn(ACTOR_URN_STRING);
  }

  @Test
  public void testExtractGraphEdgesBasic() {
    // Create test data
    UpstreamLineage upstreamLineage = createBasicUpstreamLineage();
    MetadataChangeLog event = createBasicEvent(upstreamLineage, true);

    RelationshipFieldSpec relationshipSpec = createBasicRelationshipSpec();
    List<Object> destinationUrns = Arrays.asList(upstreamUrn);

    Map.Entry<RelationshipFieldSpec, List<Object>> entry =
        new AbstractMap.SimpleEntry<>(relationshipSpec, destinationUrns);

    // Execute
    List<Edge> edges =
        GraphIndexUtils.extractGraphEdges(entry, upstreamLineage, datasetUrn, event, true);

    // Verify
    assertEquals(edges.size(), 1);
    Edge edge = edges.get(0);
    assertEquals(edge.getSource(), datasetUrn);
    assertEquals(edge.getDestination(), upstreamUrn);
    assertEquals(edge.getRelationshipType(), RELATIONSHIP_NAME);
    assertNotNull(edge.getCreatedOn());
    assertNotNull(edge.getCreatedActor());
  }

  @Test
  public void testExtractGraphEdgesWithMultipleDestinations() {
    // Create test data with multiple destinations
    Urn secondUpstreamUrn =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,upstream2.table,PROD)");

    UpstreamLineage upstreamLineage = createUpstreamLineageWithMultiple();
    MetadataChangeLog event = createBasicEvent(upstreamLineage, true);

    RelationshipFieldSpec relationshipSpec = createBasicRelationshipSpec();
    List<Object> destinationUrns = Arrays.asList(upstreamUrn, secondUpstreamUrn);

    Map.Entry<RelationshipFieldSpec, List<Object>> entry =
        new AbstractMap.SimpleEntry<>(relationshipSpec, destinationUrns);

    // Execute
    List<Edge> edges =
        GraphIndexUtils.extractGraphEdges(entry, upstreamLineage, datasetUrn, event, true);

    // Verify
    assertEquals(edges.size(), 2);
    assertTrue(edges.stream().anyMatch(edge -> edge.getDestination().equals(upstreamUrn)));
    assertTrue(edges.stream().anyMatch(edge -> edge.getDestination().equals(secondUpstreamUrn)));
  }

  @Test
  public void testExtractGraphEdgesWithSystemMetadataFallback() {
    // Create test data without audit stamps in the aspect
    UpstreamLineage upstreamLineage = createUpstreamLineageWithoutAuditStamp();
    MetadataChangeLog event = createEventWithSystemMetadata(upstreamLineage);

    RelationshipFieldSpec relationshipSpec = createBasicRelationshipSpec();
    List<Object> destinationUrns = Arrays.asList(upstreamUrn);

    Map.Entry<RelationshipFieldSpec, List<Object>> entry =
        new AbstractMap.SimpleEntry<>(relationshipSpec, destinationUrns);

    // Execute
    List<Edge> edges =
        GraphIndexUtils.extractGraphEdges(entry, upstreamLineage, datasetUrn, event, true);

    // Verify
    assertEquals(edges.size(), 1);
    Edge edge = edges.get(0);
    assertEquals(edge.getCreatedOn(), Long.valueOf(12345L)); // From system metadata
    assertEquals(edge.getUpdatedOn(), Long.valueOf(12345L)); // From system metadata
  }

  @Test
  public void testExtractGraphEdgesWithEventCreatedActor() {
    // Create test data without created actor in aspect
    UpstreamLineage upstreamLineage = createUpstreamLineageWithoutCreatedActor();
    MetadataChangeLog event = createEventWithCreatedActor(upstreamLineage);

    RelationshipFieldSpec relationshipSpec = createBasicRelationshipSpec();
    List<Object> destinationUrns = Arrays.asList(upstreamUrn);

    Map.Entry<RelationshipFieldSpec, List<Object>> entry =
        new AbstractMap.SimpleEntry<>(relationshipSpec, destinationUrns);

    // Execute
    List<Edge> edges =
        GraphIndexUtils.extractGraphEdges(entry, upstreamLineage, datasetUrn, event, true);

    // Verify
    assertEquals(edges.size(), 1);
    Edge edge = edges.get(0);
    assertEquals(edge.getCreatedActor(), actorUrn); // From event
    assertEquals(edge.getUpdatedActor(), actorUrn); // From event
  }

  @Test
  public void testExtractGraphEdgesEmptyDestinations() {
    // Test with empty destination list
    UpstreamLineage upstreamLineage = createBasicUpstreamLineage();
    MetadataChangeLog event = createBasicEvent(upstreamLineage, true);

    RelationshipFieldSpec relationshipSpec = createBasicRelationshipSpec();
    List<Object> destinationUrns = Arrays.asList(); // Empty list

    Map.Entry<RelationshipFieldSpec, List<Object>> entry =
        new AbstractMap.SimpleEntry<>(relationshipSpec, destinationUrns);

    // Execute
    List<Edge> edges =
        GraphIndexUtils.extractGraphEdges(entry, upstreamLineage, datasetUrn, event, true);

    // Verify - should return empty list for empty destinations
    assertEquals(edges.size(), 0);
  }

  @Test
  public void testExtractGraphEdgesWithPreviousSystemMetadata() {
    // Test when isNewAspectVersion is false
    UpstreamLineage upstreamLineage = createBasicUpstreamLineage();
    MetadataChangeLog event = createEventWithPreviousSystemMetadata(upstreamLineage);

    RelationshipFieldSpec relationshipSpec = createBasicRelationshipSpec();
    List<Object> destinationUrns = Arrays.asList(upstreamUrn);

    Map.Entry<RelationshipFieldSpec, List<Object>> entry =
        new AbstractMap.SimpleEntry<>(relationshipSpec, destinationUrns);

    // Execute with isNewAspectVersion = false
    List<Edge> edges =
        GraphIndexUtils.extractGraphEdges(entry, upstreamLineage, datasetUrn, event, false);

    // Verify
    assertEquals(edges.size(), 1);
    Edge edge = edges.get(0);
    // Should use previous system metadata timestamp
    assertEquals(edge.getCreatedOn(), Long.valueOf(54321L));
  }

  @Test
  public void testExtractGraphEdgesWithProperties() {
    // Test with null properties path (common case for aspects like UpstreamLineage)
    UpstreamLineage upstreamLineage = createBasicUpstreamLineage();
    MetadataChangeLog event = createBasicEvent(upstreamLineage, true);

    RelationshipFieldSpec relationshipSpec = createRelationshipSpecWithProperties();
    List<Object> destinationUrns = Arrays.asList(upstreamUrn);

    Map.Entry<RelationshipFieldSpec, List<Object>> entry =
        new AbstractMap.SimpleEntry<>(relationshipSpec, destinationUrns);

    // Execute
    List<Edge> edges =
        GraphIndexUtils.extractGraphEdges(entry, upstreamLineage, datasetUrn, event, true);

    // Verify
    assertEquals(edges.size(), 1);
    Edge edge = edges.get(0);
    // Since properties path is null, edge properties should be null
    assertNull(edge.getProperties());
  }

  @Test
  public void testExtractGraphEdgesWithViaNode() {
    // Test with null via path (common case for aspects like UpstreamLineage)
    UpstreamLineage upstreamLineage = createBasicUpstreamLineage();
    MetadataChangeLog event = createBasicEvent(upstreamLineage, true);

    RelationshipFieldSpec relationshipSpec = createRelationshipSpecWithVia();
    List<Object> destinationUrns = Arrays.asList(upstreamUrn);

    Map.Entry<RelationshipFieldSpec, List<Object>> entry =
        new AbstractMap.SimpleEntry<>(relationshipSpec, destinationUrns);

    // Execute
    List<Edge> edges =
        GraphIndexUtils.extractGraphEdges(entry, upstreamLineage, datasetUrn, event, true);

    // Verify
    assertEquals(edges.size(), 1);
    Edge edge = edges.get(0);
    // Since via path is null, edge via should be null
    assertNull(edge.getVia());
  }

  @Test
  public void testMergeEdgesBasic() {
    // Create old and new edges
    Edge oldEdge =
        new Edge(
            datasetUrn,
            upstreamUrn,
            RELATIONSHIP_NAME,
            1000L, // createdOn
            actorUrn, // createdActor
            2000L, // updatedOn
            actorUrn, // updatedActor
            Collections.singletonMap("oldKey", "oldValue"), // properties
            null, // lifecycleOwner
            null // via
            );

    Urn newActorUrn = UrnUtils.getUrn("urn:li:corpuser:newuser");
    Edge newEdge =
        new Edge(
            datasetUrn,
            upstreamUrn,
            RELATIONSHIP_NAME,
            3000L, // createdOn (will be ignored)
            newActorUrn, // createdActor (will be ignored)
            4000L, // updatedOn
            newActorUrn, // updatedActor
            Collections.singletonMap("newKey", "newValue"), // properties
            null, // lifecycleOwner
            null // via
            );

    // Execute
    Edge mergedEdge = GraphIndexUtils.mergeEdges(oldEdge, newEdge);

    // Verify
    assertEquals(mergedEdge.getSource(), oldEdge.getSource());
    assertEquals(mergedEdge.getDestination(), oldEdge.getDestination());
    assertEquals(mergedEdge.getRelationshipType(), oldEdge.getRelationshipType());

    // Created fields should be null (not copied from either edge)
    assertNull(mergedEdge.getCreatedOn());
    assertNull(mergedEdge.getCreatedActor());

    // Updated fields should come from new edge
    assertEquals(mergedEdge.getUpdatedOn(), newEdge.getUpdatedOn());
    assertEquals(mergedEdge.getUpdatedActor(), newEdge.getUpdatedActor());
    assertEquals(mergedEdge.getProperties(), newEdge.getProperties());

    // Other fields should come from old edge
    assertEquals(mergedEdge.getLifecycleOwner(), oldEdge.getLifecycleOwner());
    assertEquals(mergedEdge.getVia(), oldEdge.getVia());
  }

  @Test
  public void testMergeEdgesWithAllFieldsFromOldEdge() {
    // Create edges with all optional fields populated
    Urn lifecycleOwnerUrn = UrnUtils.getUrn("urn:li:corpuser:owner");
    Urn viaUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,via.table,PROD)");

    Edge oldEdge =
        new Edge(
            datasetUrn, // source
            upstreamUrn, // destination
            RELATIONSHIP_NAME, // relationshipType
            1000L, // createdOn
            actorUrn, // createdActor
            2000L, // updatedOn
            actorUrn, // updatedActor
            Collections.singletonMap("oldKey", "oldValue"), // properties
            lifecycleOwnerUrn, // lifecycleOwner
            viaUrn, // via
            true, // sourceStatus
            true, // destinationStatus
            true, // viaStatus
            true // lifecycleOwnerStatus
            );

    Edge newEdge =
        new Edge(
            datasetUrn,
            upstreamUrn,
            RELATIONSHIP_NAME,
            3000L,
            UrnUtils.getUrn("urn:li:corpuser:newuser"),
            4000L,
            UrnUtils.getUrn("urn:li:corpuser:newuser"),
            Collections.singletonMap("newKey", "newValue"),
            null, // Different lifecycle owner (will be ignored)
            null // Different via (will be ignored)
            );

    // Execute
    Edge mergedEdge = GraphIndexUtils.mergeEdges(oldEdge, newEdge);

    // Verify that all old edge fields are preserved
    assertEquals(mergedEdge.getLifecycleOwner(), oldEdge.getLifecycleOwner());
    assertEquals(mergedEdge.getVia(), oldEdge.getVia());
    assertEquals(mergedEdge.getViaStatus(), oldEdge.getViaStatus());
    assertEquals(mergedEdge.getLifecycleOwnerStatus(), oldEdge.getLifecycleOwnerStatus());
    assertEquals(mergedEdge.getSourceStatus(), oldEdge.getSourceStatus());
    assertEquals(mergedEdge.getDestinationStatus(), oldEdge.getDestinationStatus());
  }

  // Helper methods for creating test data

  private UpstreamLineage createBasicUpstreamLineage() {
    UpstreamLineage upstreamLineage = new UpstreamLineage();
    UpstreamArray upstreams = new UpstreamArray();

    Upstream upstream = new Upstream();
    upstream.setDataset(UrnUtils.toDatasetUrn("mysql", "upstream.table", "PROD"));
    upstream.setType(DatasetLineageType.TRANSFORMED);
    upstream.setAuditStamp(new AuditStamp().setTime(12345L).setActor(actorUrn));

    upstreams.add(upstream);
    upstreamLineage.setUpstreams(upstreams);

    return upstreamLineage;
  }

  private UpstreamLineage createUpstreamLineageWithMultiple() {
    UpstreamLineage upstreamLineage = new UpstreamLineage();
    UpstreamArray upstreams = new UpstreamArray();

    Upstream upstream1 = new Upstream();
    upstream1.setDataset(UrnUtils.toDatasetUrn("mysql", "upstream.table", "PROD"));
    upstream1.setType(DatasetLineageType.TRANSFORMED);
    upstream1.setAuditStamp(new AuditStamp().setTime(12345L).setActor(actorUrn));

    Upstream upstream2 = new Upstream();
    upstream2.setDataset(UrnUtils.toDatasetUrn("mysql", "upstream2.table", "PROD"));
    upstream2.setType(DatasetLineageType.TRANSFORMED);
    upstream2.setAuditStamp(new AuditStamp().setTime(12346L).setActor(actorUrn));

    upstreams.add(upstream1);
    upstreams.add(upstream2);
    upstreamLineage.setUpstreams(upstreams);

    return upstreamLineage;
  }

  private UpstreamLineage createUpstreamLineageWithoutAuditStamp() {
    UpstreamLineage upstreamLineage = new UpstreamLineage();
    UpstreamArray upstreams = new UpstreamArray();

    Upstream upstream = new Upstream();
    upstream.setDataset(UrnUtils.toDatasetUrn("mysql", "upstream.table", "PROD"));
    upstream.setType(DatasetLineageType.TRANSFORMED);
    // No audit stamp set

    upstreams.add(upstream);
    upstreamLineage.setUpstreams(upstreams);

    return upstreamLineage;
  }

  private UpstreamLineage createUpstreamLineageWithoutCreatedActor() {
    UpstreamLineage upstreamLineage = new UpstreamLineage();
    UpstreamArray upstreams = new UpstreamArray();

    Upstream upstream = new Upstream();
    upstream.setDataset(UrnUtils.toDatasetUrn("mysql", "upstream.table", "PROD"));
    upstream.setType(DatasetLineageType.TRANSFORMED);
    upstream.setAuditStamp(new AuditStamp().setTime(12345L)); // No actor

    upstreams.add(upstream);
    upstreamLineage.setUpstreams(upstreams);

    return upstreamLineage;
  }

  private MetadataChangeLog createBasicEvent(RecordTemplate aspect, boolean isNew) {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityUrn(datasetUrn);
    event.setEntityType(Constants.DATASET_ENTITY_NAME);
    event.setAspectName(Constants.UPSTREAM_LINEAGE_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setAspect(GenericRecordUtils.serializeAspect(aspect));

    // Add system metadata for timestamp fallback
    SystemMetadata systemMetadata = new SystemMetadata();
    systemMetadata.setLastObserved(12345L);
    event.setSystemMetadata(systemMetadata);

    // Add created audit stamp for actor fallback
    com.linkedin.common.AuditStamp createdStamp = new com.linkedin.common.AuditStamp();
    createdStamp.setTime(12345L);
    createdStamp.setActor(actorUrn);
    event.setCreated(createdStamp);

    return event;
  }

  private MetadataChangeLog createEventWithSystemMetadata(RecordTemplate aspect) {
    MetadataChangeLog event = createBasicEvent(aspect, true);

    SystemMetadata systemMetadata = new SystemMetadata();
    systemMetadata.setLastObserved(12345L);
    event.setSystemMetadata(systemMetadata);

    return event;
  }

  private MetadataChangeLog createEventWithPreviousSystemMetadata(RecordTemplate aspect) {
    MetadataChangeLog event = createBasicEvent(aspect, false);

    SystemMetadata previousSystemMetadata = new SystemMetadata();
    previousSystemMetadata.setLastObserved(54321L);
    event.setPreviousSystemMetadata(previousSystemMetadata);

    return event;
  }

  private MetadataChangeLog createEventWithCreatedActor(RecordTemplate aspect) {
    MetadataChangeLog event = createBasicEvent(aspect, true);

    AuditStamp createdStamp = new AuditStamp();
    createdStamp.setTime(12345L);
    createdStamp.setActor(actorUrn);
    event.setCreated(createdStamp);

    return event;
  }

  private RelationshipFieldSpec createBasicRelationshipSpec() {
    RelationshipFieldSpec spec = mock(RelationshipFieldSpec.class);
    RelationshipAnnotation annotation = mock(RelationshipAnnotation.class);

    when(spec.getRelationshipName()).thenReturn(RELATIONSHIP_NAME);
    when(spec.getRelationshipAnnotation()).thenReturn(annotation);

    // Mock annotation methods to return null for optional paths
    when(annotation.getCreatedOn()).thenReturn(null);
    when(annotation.getCreatedActor()).thenReturn(null);
    when(annotation.getUpdatedOn()).thenReturn(null);
    when(annotation.getUpdatedActor()).thenReturn(null);
    when(annotation.getProperties()).thenReturn(null);
    when(annotation.getVia()).thenReturn(null);

    return spec;
  }

  private RelationshipFieldSpec createRelationshipSpecWithProperties() {
    RelationshipFieldSpec spec = createBasicRelationshipSpec();
    RelationshipAnnotation annotation = spec.getRelationshipAnnotation();

    // For UpstreamLineage, there's no properties field, so we test with null path
    // This tests that the code handles null properties path gracefully
    when(annotation.getProperties()).thenReturn(null);

    return spec;
  }

  private RelationshipFieldSpec createRelationshipSpecWithVia() {
    RelationshipFieldSpec spec = createBasicRelationshipSpec();
    RelationshipAnnotation annotation = spec.getRelationshipAnnotation();

    // For UpstreamLineage, there's no via field, so we test with null path
    // This tests that the code handles null via path gracefully
    when(annotation.getVia()).thenReturn(null);

    return spec;
  }
}
