package com.linkedin.metadata.models.registry;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.annotation.RelationshipAnnotation;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class LineageRegistryTest {
  @Test
  public void testRegistryWhenEmpty() {
    EntityRegistry entityRegistry = mock(EntityRegistry.class);
    when(entityRegistry.getEntitySpecs()).thenReturn(Collections.emptyMap());
    LineageRegistry lineageRegistry = new LineageRegistry(entityRegistry);
    LineageRegistry.LineageSpec lineageSpec = lineageRegistry.getLineageSpec("dataset");
    assertNull(lineageSpec);
  }

  @Test
  public void testRegistry() {
    Map<String, EntitySpec> mockEntitySpecs = new HashMap<>();
    EntitySpec mockDatasetSpec = mock(EntitySpec.class);
    List<RelationshipFieldSpec> datasetRelations =
        ImmutableList.of(
            buildSpec("DownstreamOf", ImmutableList.of("dataset"), true, true),
            buildSpec("AssociatedWith", ImmutableList.of("tag"), true, false),
            buildSpec("AssociatedWith", ImmutableList.of("glossaryTerm"), true, false));
    when(mockDatasetSpec.getRelationshipFieldSpecs()).thenReturn(datasetRelations);
    mockEntitySpecs.put("dataset", mockDatasetSpec);
    EntitySpec mockJobSpec = mock(EntitySpec.class);
    List<RelationshipFieldSpec> jobRelations =
        ImmutableList.of(
            buildSpec("Produces", ImmutableList.of("dataset"), false, true),
            buildSpec("Consumes", ImmutableList.of("dataset"), true, true));
    when(mockJobSpec.getRelationshipFieldSpecs()).thenReturn(jobRelations);
    mockEntitySpecs.put("dataJob", mockJobSpec);
    EntityRegistry entityRegistry = mock(EntityRegistry.class);
    when(entityRegistry.getEntitySpecs()).thenReturn(mockEntitySpecs);

    LineageRegistry lineageRegistry = new LineageRegistry(entityRegistry);
    LineageRegistry.LineageSpec lineageSpec = lineageRegistry.getLineageSpec("dataset");
    assertEquals(lineageSpec.getUpstreamEdges().size(), 2);
    assertTrue(
        lineageSpec
            .getUpstreamEdges()
            .contains(
                new LineageRegistry.EdgeInfo(
                    "DownstreamOf", RelationshipDirection.OUTGOING, "dataset")));
    assertTrue(
        lineageSpec
            .getUpstreamEdges()
            .contains(
                new LineageRegistry.EdgeInfo(
                    "Produces", RelationshipDirection.INCOMING, "dataJob")));
    assertEquals(lineageSpec.getDownstreamEdges().size(), 2);
    assertTrue(
        lineageSpec
            .getDownstreamEdges()
            .contains(
                new LineageRegistry.EdgeInfo(
                    "DownstreamOf", RelationshipDirection.INCOMING, "dataset")));
    assertTrue(
        lineageSpec
            .getDownstreamEdges()
            .contains(
                new LineageRegistry.EdgeInfo(
                    "Consumes", RelationshipDirection.INCOMING, "dataJob")));
  }

  private RelationshipFieldSpec buildSpec(
      String relationshipType,
      List<String> destinationEntityTypes,
      boolean isUpstream,
      boolean isLineage) {
    RelationshipFieldSpec spec = mock(RelationshipFieldSpec.class);
    when(spec.getRelationshipAnnotation())
        .thenReturn(
            new RelationshipAnnotation(
                relationshipType,
                destinationEntityTypes,
                isUpstream,
                isLineage,
                null,
                null,
                null,
                null,
                null));
    return spec;
  }
}
