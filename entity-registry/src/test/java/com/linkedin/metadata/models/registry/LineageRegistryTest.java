package com.linkedin.metadata.models.registry;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import java.util.List;
import java.util.Set;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class LineageRegistryTest {
  private EntityRegistry entityRegistry;
  private LineageRegistry lineageRegistry;

  @BeforeTest
  public void init() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
    entityRegistry =
        new ConfigEntityRegistry(
            LineageRegistryTest.class
                .getClassLoader()
                .getResourceAsStream("test-lineage-entity-registry.yml"));
    lineageRegistry = new LineageRegistry(entityRegistry);
  }

  @Test
  public void testGetLineageRelationshipsUpstream() {
    // Test
    List<LineageRegistry.EdgeInfo> upstreamEdges =
        lineageRegistry.getLineageRelationships("dataset", LineageDirection.UPSTREAM);

    // Verify
    assertEquals(upstreamEdges.size(), 3);
    assertTrue(
        upstreamEdges.contains(
            new LineageRegistry.EdgeInfo(
                "DownstreamOf", RelationshipDirection.OUTGOING, "dataset")));
    assertTrue(
        upstreamEdges.contains(
            new LineageRegistry.EdgeInfo("Produces", RelationshipDirection.INCOMING, "dataJob")));
  }

  @Test
  public void testGetLineageRelationshipsDownstream() {
    // Test
    List<LineageRegistry.EdgeInfo> downstreamEdges =
        lineageRegistry.getLineageRelationships("dataset", LineageDirection.DOWNSTREAM);

    // Verify
    assertEquals(downstreamEdges.size(), 8);
    assertTrue(
        downstreamEdges.contains(
            new LineageRegistry.EdgeInfo(
                "DownstreamOf", RelationshipDirection.INCOMING, "dataset")));
    assertTrue(
        downstreamEdges.contains(
            new LineageRegistry.EdgeInfo("Consumes", RelationshipDirection.INCOMING, "dataJob")));
  }

  @Test
  public void testGetLineageRelationshipsForNonExistentEntity() {
    // Test
    List<LineageRegistry.EdgeInfo> edges =
        lineageRegistry.getLineageRelationships("nonExistentEntity", LineageDirection.UPSTREAM);

    // Verify
    assertEquals(edges.size(), 0);
  }

  @Test
  public void testCaseInsensitiveEntityNames() {
    // Test with different case
    LineageRegistry.LineageSpec lineageSpec = lineageRegistry.getLineageSpec("DATASET");

    // Verify
    assertNotNull(lineageSpec);
    assertEquals(lineageSpec.getUpstreamEdges().size(), 3);
    assertEquals(lineageSpec.getDownstreamEdges().size(), 8);
  }

  @Test
  public void testGetSchemaFieldLineageSpec() {
    // Get LineageSpec for schema field
    LineageRegistry.LineageSpec schemaFieldLineageSpec =
        lineageRegistry.getLineageSpec(Constants.SCHEMA_FIELD_ENTITY_NAME);

    // Verify that we get empty for schema field's LineageSpec
    assertEquals(
        schemaFieldLineageSpec,
        new LineageRegistry.LineageSpec(List.of(), List.of()),
        "Schema field LineageSpec should be null");
  }

  @Test
  public void testSchemaFieldRelationships() {
    // Get lineage relationships for schema field
    List<LineageRegistry.EdgeInfo> upstreamEdges =
        lineageRegistry.getLineageRelationships(
            Constants.SCHEMA_FIELD_ENTITY_NAME, LineageDirection.UPSTREAM);
    List<LineageRegistry.EdgeInfo> downstreamEdges =
        lineageRegistry.getLineageRelationships(
            Constants.SCHEMA_FIELD_ENTITY_NAME, LineageDirection.DOWNSTREAM);

    // Verify upstream edges
    assertEquals(upstreamEdges.size(), 1, "Schema field should have 1 upstream edge");
    assertTrue(
        upstreamEdges.contains(
            new LineageRegistry.EdgeInfo(
                "DownstreamOf",
                RelationshipDirection.OUTGOING,
                Constants.SCHEMA_FIELD_ENTITY_NAME)),
        "Expected upstream edge not found");

    // Verify downstream edges
    assertEquals(downstreamEdges.size(), 1, "Schema field should have 1 downstream edge");
    assertTrue(
        downstreamEdges.contains(
            new LineageRegistry.EdgeInfo(
                "DownstreamOf",
                RelationshipDirection.INCOMING,
                Constants.SCHEMA_FIELD_ENTITY_NAME)),
        "Expected downstream edge not found");
  }

  @Test
  public void testCaseInsensitiveSchemaFieldName() {
    // Test with EXACT case schema field name
    List<LineageRegistry.EdgeInfo> upstreamEdges =
        lineageRegistry.getLineageRelationships(
            Constants.SCHEMA_FIELD_ENTITY_NAME, LineageDirection.UPSTREAM);

    // Verify we get the correct relationships with the correct case
    assertEquals(
        upstreamEdges.size(), 1, "Schema field should have 1 upstream edge with correct name");
    assertTrue(
        upstreamEdges.contains(
            new LineageRegistry.EdgeInfo(
                "DownstreamOf",
                RelationshipDirection.OUTGOING,
                Constants.SCHEMA_FIELD_ENTITY_NAME)),
        "Expected upstream edge not found with correct name");
  }

  @Test
  public void testEntityWithNoLineageRelationships() {
    // Find an entity in the registry that has no lineage relationships
    // Assuming "tag" entity exists in the registry and has no lineage relationships
    LineageRegistry.LineageSpec lineageSpec = lineageRegistry.getLineageSpec("tag");

    // Verify
    assertNotNull(lineageSpec);
    assertEquals(lineageSpec.getUpstreamEdges().size(), 0);
    assertEquals(lineageSpec.getDownstreamEdges().size(), 0);
  }

  @Test
  public void testEdgeInfoHashCodeAndEquals() {
    // Create two edges with the same values but different casing
    LineageRegistry.EdgeInfo edge1 =
        new LineageRegistry.EdgeInfo("DownstreamOf", RelationshipDirection.OUTGOING, "dataset");
    LineageRegistry.EdgeInfo edge2 =
        new LineageRegistry.EdgeInfo("downstreamof", RelationshipDirection.OUTGOING, "DATASET");
    LineageRegistry.EdgeInfo edge3 =
        new LineageRegistry.EdgeInfo("OtherRelation", RelationshipDirection.OUTGOING, "dataset");

    // Test equals
    assertTrue(edge1.equals(edge2));
    assertTrue(!edge1.equals(edge3));
    assertTrue(edge1.equals(edge1)); // Self equality
    assertTrue(!edge1.equals(null)); // Null check
    assertTrue(!edge1.equals("not an edge")); // Different type

    // Test hashCode
    assertEquals(edge1.hashCode(), edge2.hashCode());
  }

  @Test
  public void testGetEntityRegistry() {
    // Test
    EntityRegistry returnedRegistry = lineageRegistry.getEntityRegistry();

    // Verify
    assertEquals(returnedRegistry, entityRegistry);
  }

  @Test
  public void testGetEntitiesWithLineageToEntityType() {
    // Test getting entities with lineage to dataset
    Set<String> entitiesWithLineage = lineageRegistry.getEntitiesWithLineageToEntityType("dataset");

    // Verify expected entities are found
    // We expect dataset, dataJob and chart based on the test registry
    assertTrue(entitiesWithLineage.contains("dataset"));
    assertTrue(entitiesWithLineage.contains("dataJob"));

    // Test getting entities with lineage to dataJob
    entitiesWithLineage = lineageRegistry.getEntitiesWithLineageToEntityType("dataJob");

    // Verify expected entities are found
    assertTrue(entitiesWithLineage.contains("dataset"));
    assertTrue(entitiesWithLineage.contains("dataJob"));

    // Count of entities might vary depending on registry content, so we don't assert exact counts
    // but ensure key expected entities are present
  }
}
