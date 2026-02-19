package datahub.hive.producer;

import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.hive.ql.optimizer.lineage.LineageCtx.Index;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for HiveLineageLogger class.
 * Tests focus on the public static methods and inner classes that are marked as @VisibleForTesting.
 */
class HiveLineageLoggerTest {

    @BeforeEach
    void setUp() {
        // Setup before each test
    }

    @Test
    void testVertex_creation() {
        // Given & When
        HiveLineageLogger.Vertex vertex = new HiveLineageLogger.Vertex("db.table.column");
        
        // Then
        assertNotNull(vertex);
        assertEquals("db.table.column", vertex.getLabel());
        assertEquals(HiveLineageLogger.Vertex.Type.COLUMN, vertex.getType());
    }

    @Test
    void testVertex_creationWithType() {
        // Given & When
        HiveLineageLogger.Vertex vertex = new HiveLineageLogger.Vertex("db.table", HiveLineageLogger.Vertex.Type.TABLE);
        
        // Then
        assertNotNull(vertex);
        assertEquals("db.table", vertex.getLabel());
        assertEquals(HiveLineageLogger.Vertex.Type.TABLE, vertex.getType());
    }

    @Test
    void testVertex_equality() {
        // Given
        HiveLineageLogger.Vertex vertex1 = new HiveLineageLogger.Vertex("db.table.column");
        HiveLineageLogger.Vertex vertex2 = new HiveLineageLogger.Vertex("db.table.column");
        HiveLineageLogger.Vertex vertex3 = new HiveLineageLogger.Vertex("db.table.other");
        
        // Then
        assertEquals(vertex1, vertex2);
        assertNotEquals(vertex1, vertex3);
        assertEquals(vertex1.hashCode(), vertex2.hashCode());
    }

    @Test
    void testVertex_equalityWithDifferentTypes() {
        // Given
        HiveLineageLogger.Vertex vertex1 = new HiveLineageLogger.Vertex("db.table", HiveLineageLogger.Vertex.Type.COLUMN);
        HiveLineageLogger.Vertex vertex2 = new HiveLineageLogger.Vertex("db.table", HiveLineageLogger.Vertex.Type.TABLE);
        
        // Then
        assertNotEquals(vertex1, vertex2);
    }

    @Test
    void testVertex_equalsSameObject() {
        // Given
        HiveLineageLogger.Vertex vertex = new HiveLineageLogger.Vertex("db.table.column");
        
        // Then
        assertEquals(vertex, vertex);
    }

    @Test
    void testVertex_equalsNull() {
        // Given
        HiveLineageLogger.Vertex vertex = new HiveLineageLogger.Vertex("db.table.column");
        
        // Then
        assertNotEquals(vertex, null);
    }

    @Test
    void testVertex_equalsDifferentClass() {
        // Given
        HiveLineageLogger.Vertex vertex = new HiveLineageLogger.Vertex("db.table.column");
        String notAVertex = "db.table.column";
        
        // Then
        assertNotEquals(vertex, notAVertex);
    }

    @Test
    void testEdge_creation() {
        // Given
        Set<HiveLineageLogger.Vertex> sources = new LinkedHashSet<>();
        sources.add(new HiveLineageLogger.Vertex("source.table.col1"));
        
        Set<HiveLineageLogger.Vertex> targets = new LinkedHashSet<>();
        targets.add(new HiveLineageLogger.Vertex("target.table.col1"));
        
        String expression = "col1 + 1";
        
        // When
        HiveLineageLogger.Edge edge = new HiveLineageLogger.Edge(
                sources, 
                targets, 
                expression, 
                HiveLineageLogger.Edge.Type.PROJECTION
        );
        
        // Then
        assertNotNull(edge);
    }

    @Test
    void testEdge_projectionType() {
        // Given
        Set<HiveLineageLogger.Vertex> sources = new LinkedHashSet<>();
        Set<HiveLineageLogger.Vertex> targets = new LinkedHashSet<>();
        
        // When
        HiveLineageLogger.Edge edge = new HiveLineageLogger.Edge(
                sources, 
                targets, 
                "expression", 
                HiveLineageLogger.Edge.Type.PROJECTION
        );
        
        // Then
        assertNotNull(edge);
    }

    @Test
    void testEdge_predicateType() {
        // Given
        Set<HiveLineageLogger.Vertex> sources = new LinkedHashSet<>();
        Set<HiveLineageLogger.Vertex> targets = new LinkedHashSet<>();
        
        // When
        HiveLineageLogger.Edge edge = new HiveLineageLogger.Edge(
                sources, 
                targets, 
                "WHERE condition", 
                HiveLineageLogger.Edge.Type.PREDICATE
        );
        
        // Then
        assertNotNull(edge);
    }

    @Test
    void testGetVertices_emptyEdges() {
        // Given
        List<HiveLineageLogger.Edge> edges = new ArrayList<>();
        
        // When
        Set<HiveLineageLogger.Vertex> vertices = HiveLineageLogger.getVertices(edges);
        
        // Then
        assertNotNull(vertices);
        assertTrue(vertices.isEmpty());
    }

    @Test
    void testGetVertices_singleEdge() {
        // Given
        HiveLineageLogger.Vertex source = new HiveLineageLogger.Vertex("source.table.col1");
        HiveLineageLogger.Vertex target = new HiveLineageLogger.Vertex("target.table.col1");
        
        Set<HiveLineageLogger.Vertex> sources = new LinkedHashSet<>();
        sources.add(source);
        
        Set<HiveLineageLogger.Vertex> targets = new LinkedHashSet<>();
        targets.add(target);
        
        List<HiveLineageLogger.Edge> edges = new ArrayList<>();
        edges.add(new HiveLineageLogger.Edge(sources, targets, "expr", HiveLineageLogger.Edge.Type.PROJECTION));
        
        // When
        Set<HiveLineageLogger.Vertex> vertices = HiveLineageLogger.getVertices(edges);
        
        // Then
        assertNotNull(vertices);
        assertEquals(2, vertices.size());
        assertTrue(vertices.contains(source));
        assertTrue(vertices.contains(target));
    }

    @Test
    void testGetVertices_multipleEdges() {
        // Given
        HiveLineageLogger.Vertex source1 = new HiveLineageLogger.Vertex("source1.table.col1");
        HiveLineageLogger.Vertex source2 = new HiveLineageLogger.Vertex("source2.table.col2");
        HiveLineageLogger.Vertex target1 = new HiveLineageLogger.Vertex("target.table.col1");
        HiveLineageLogger.Vertex target2 = new HiveLineageLogger.Vertex("target.table.col2");
        
        Set<HiveLineageLogger.Vertex> sources1 = new LinkedHashSet<>();
        sources1.add(source1);
        
        Set<HiveLineageLogger.Vertex> targets1 = new LinkedHashSet<>();
        targets1.add(target1);
        
        Set<HiveLineageLogger.Vertex> sources2 = new LinkedHashSet<>();
        sources2.add(source2);
        
        Set<HiveLineageLogger.Vertex> targets2 = new LinkedHashSet<>();
        targets2.add(target2);
        
        List<HiveLineageLogger.Edge> edges = new ArrayList<>();
        edges.add(new HiveLineageLogger.Edge(sources1, targets1, "expr1", HiveLineageLogger.Edge.Type.PROJECTION));
        edges.add(new HiveLineageLogger.Edge(sources2, targets2, "expr2", HiveLineageLogger.Edge.Type.PROJECTION));
        
        // When
        Set<HiveLineageLogger.Vertex> vertices = HiveLineageLogger.getVertices(edges);
        
        // Then
        assertNotNull(vertices);
        assertEquals(4, vertices.size());
    }

    @Test
    void testGetVertices_duplicateVertices() {
        // Given
        HiveLineageLogger.Vertex source = new HiveLineageLogger.Vertex("source.table.col1");
        HiveLineageLogger.Vertex target = new HiveLineageLogger.Vertex("target.table.col1");
        
        Set<HiveLineageLogger.Vertex> sources1 = new LinkedHashSet<>();
        sources1.add(source);
        
        Set<HiveLineageLogger.Vertex> targets1 = new LinkedHashSet<>();
        targets1.add(target);
        
        Set<HiveLineageLogger.Vertex> sources2 = new LinkedHashSet<>();
        sources2.add(source); // Same source vertex
        
        Set<HiveLineageLogger.Vertex> targets2 = new LinkedHashSet<>();
        targets2.add(target); // Same target vertex
        
        List<HiveLineageLogger.Edge> edges = new ArrayList<>();
        edges.add(new HiveLineageLogger.Edge(sources1, targets1, "expr1", HiveLineageLogger.Edge.Type.PROJECTION));
        edges.add(new HiveLineageLogger.Edge(sources2, targets2, "expr2", HiveLineageLogger.Edge.Type.PROJECTION));
        
        // When
        Set<HiveLineageLogger.Vertex> vertices = HiveLineageLogger.getVertices(edges);
        
        // Then
        assertNotNull(vertices);
        assertEquals(2, vertices.size()); // Should only have 2 unique vertices
    }

    @Test
    void testGetVertices_assignsIds() {
        // Given
        HiveLineageLogger.Vertex source = new HiveLineageLogger.Vertex("source.table.col1");
        HiveLineageLogger.Vertex target = new HiveLineageLogger.Vertex("target.table.col1");
        
        Set<HiveLineageLogger.Vertex> sources = new LinkedHashSet<>();
        sources.add(source);
        
        Set<HiveLineageLogger.Vertex> targets = new LinkedHashSet<>();
        targets.add(target);
        
        List<HiveLineageLogger.Edge> edges = new ArrayList<>();
        edges.add(new HiveLineageLogger.Edge(sources, targets, "expr", HiveLineageLogger.Edge.Type.PROJECTION));
        
        // When
        Set<HiveLineageLogger.Vertex> vertices = HiveLineageLogger.getVertices(edges);
        
        // Then
        assertNotNull(vertices);
        List<HiveLineageLogger.Vertex> vertexList = new ArrayList<>(vertices);
        assertEquals(0, vertexList.get(0).getId()); // First vertex should have id 0
        assertEquals(1, vertexList.get(1).getId()); // Second vertex should have id 1
    }

    @Test
    void testGetVertices_targetsBeforeSources() {
        // Given
        HiveLineageLogger.Vertex source = new HiveLineageLogger.Vertex("source.table.col1");
        HiveLineageLogger.Vertex target = new HiveLineageLogger.Vertex("target.table.col1");
        
        Set<HiveLineageLogger.Vertex> sources = new LinkedHashSet<>();
        sources.add(source);
        
        Set<HiveLineageLogger.Vertex> targets = new LinkedHashSet<>();
        targets.add(target);
        
        List<HiveLineageLogger.Edge> edges = new ArrayList<>();
        edges.add(new HiveLineageLogger.Edge(sources, targets, "expr", HiveLineageLogger.Edge.Type.PROJECTION));
        
        // When
        Set<HiveLineageLogger.Vertex> vertices = HiveLineageLogger.getVertices(edges);
        
        // Then - targets should come before sources in the set
        List<HiveLineageLogger.Vertex> vertexList = new ArrayList<>(vertices);
        assertEquals(target, vertexList.get(0)); // Target should be first
        assertEquals(source, vertexList.get(1)); // Source should be second
    }

    @Test
    void testVertexTypes_enum() {
        // Test that enum values exist
        assertEquals(HiveLineageLogger.Vertex.Type.COLUMN, HiveLineageLogger.Vertex.Type.valueOf("COLUMN"));
        assertEquals(HiveLineageLogger.Vertex.Type.TABLE, HiveLineageLogger.Vertex.Type.valueOf("TABLE"));
    }

    @Test
    void testEdgeTypes_enum() {
        // Test that enum values exist
        assertEquals(HiveLineageLogger.Edge.Type.PROJECTION, HiveLineageLogger.Edge.Type.valueOf("PROJECTION"));
        assertEquals(HiveLineageLogger.Edge.Type.PREDICATE, HiveLineageLogger.Edge.Type.valueOf("PREDICATE"));
    }
}
