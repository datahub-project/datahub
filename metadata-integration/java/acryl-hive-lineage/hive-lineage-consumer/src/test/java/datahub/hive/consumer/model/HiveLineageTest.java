package datahub.hive.consumer.model;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for HiveLineage model class.
 * Tests serialization, deserialization, and data integrity of the model.
 */
public class HiveLineageTest {

    private Gson gson;
    private String validHiveLineageJson;
    private HiveLineage validHiveLineage;

    @BeforeEach
    void setUp() {
        gson = new Gson();
        
        // Create a valid HiveLineage object for testing
        validHiveLineage = new HiveLineage();
        validHiveLineage.setVersion("1.0");
        validHiveLineage.setUser("testuser");
        validHiveLineage.setTimestamp(1234567890L);
        validHiveLineage.setDuration(5000L);
        validHiveLineage.setJobIds(Arrays.asList("job1", "job2"));
        validHiveLineage.setEngine("hive");
        validHiveLineage.setDatabase("testdb");
        validHiveLineage.setHash("testhash123");
        validHiveLineage.setQueryText("CREATE TABLE testdb.output_table AS SELECT * FROM testdb.input_table");
        validHiveLineage.setEnvironment("DEV");
        validHiveLineage.setPlatformInstance("urn:li:dataPlatformInstance:(urn:li:dataPlatform:hive,OPR.OCE.DEV)");
        validHiveLineage.setInputs(Arrays.asList("testdb.input_table"));
        validHiveLineage.setOutputs(Arrays.asList("testdb.output_table"));

        // Create edges
        HiveLineage.Edge edge = new HiveLineage.Edge();
        edge.setSources(Arrays.asList(0));
        edge.setTargets(Arrays.asList(1));
        edge.setExpression("col1");
        edge.setEdgeType("PROJECTION");
        validHiveLineage.setEdges(Arrays.asList(edge));

        // Create vertices
        HiveLineage.Vertex vertex1 = new HiveLineage.Vertex();
        vertex1.setId(0);
        vertex1.setVertexType("COLUMN");
        vertex1.setVertexId("input_table.col1");

        HiveLineage.Vertex vertex2 = new HiveLineage.Vertex();
        vertex2.setId(1);
        vertex2.setVertexType("COLUMN");
        vertex2.setVertexId("output_table.col1");

        validHiveLineage.setVertices(Arrays.asList(vertex1, vertex2));

        // Create corresponding JSON string
        validHiveLineageJson = "{"
            + "\"version\":\"1.0\","
            + "\"user\":\"testuser\","
            + "\"timestamp\":1234567890,"
            + "\"duration\":5000,"
            + "\"jobIds\":[\"job1\",\"job2\"],"
            + "\"engine\":\"hive\","
            + "\"database\":\"testdb\","
            + "\"hash\":\"testhash123\","
            + "\"queryText\":\"CREATE TABLE testdb.output_table AS SELECT * FROM testdb.input_table\","
            + "\"environment\":\"DEV\","
            + "\"platformInstance\":\"urn:li:dataPlatformInstance:(urn:li:dataPlatform:hive,OPR.OCE.DEV)\","
            + "\"inputs\":[\"testdb.input_table\"],"
            + "\"outputs\":[\"testdb.output_table\"],"
            + "\"edges\":[{\"sources\":[0],\"targets\":[1],\"expression\":\"col1\",\"edgeType\":\"PROJECTION\"}],"
            + "\"vertices\":[{\"id\":0,\"vertexType\":\"COLUMN\",\"vertexId\":\"input_table.col1\"},{\"id\":1,\"vertexType\":\"COLUMN\",\"vertexId\":\"output_table.col1\"}]"
            + "}";
    }

    /**
     * Tests successful deserialization of valid HiveLineage JSON.
     * Verifies that all fields are correctly mapped from JSON.
     */
    @Test
    void testDeserialization_ValidJson() {
        // Deserialize JSON to HiveLineage object
        HiveLineage hiveLineage = gson.fromJson(validHiveLineageJson, HiveLineage.class);

        // Verify all fields are correctly deserialized
        assertNotNull(hiveLineage);
        assertEquals("1.0", hiveLineage.getVersion());
        assertEquals("testuser", hiveLineage.getUser());
        assertEquals(1234567890L, hiveLineage.getTimestamp());
        assertEquals(5000L, hiveLineage.getDuration());
        assertEquals(Arrays.asList("job1", "job2"), hiveLineage.getJobIds());
        assertEquals("hive", hiveLineage.getEngine());
        assertEquals("testdb", hiveLineage.getDatabase());
        assertEquals("testhash123", hiveLineage.getHash());
        assertEquals("CREATE TABLE testdb.output_table AS SELECT * FROM testdb.input_table", hiveLineage.getQueryText());
        assertEquals("DEV", hiveLineage.getEnvironment());
        assertEquals("urn:li:dataPlatformInstance:(urn:li:dataPlatform:hive,OPR.OCE.DEV)", hiveLineage.getPlatformInstance());
        assertEquals(Arrays.asList("testdb.input_table"), hiveLineage.getInputs());
        assertEquals(Arrays.asList("testdb.output_table"), hiveLineage.getOutputs());

        // Verify edges
        assertNotNull(hiveLineage.getEdges());
        assertEquals(1, hiveLineage.getEdges().size());
        HiveLineage.Edge edge = hiveLineage.getEdges().get(0);
        assertEquals(Arrays.asList(0), edge.getSources());
        assertEquals(Arrays.asList(1), edge.getTargets());
        assertEquals("col1", edge.getExpression());
        assertEquals("PROJECTION", edge.getEdgeType());

        // Verify vertices
        assertNotNull(hiveLineage.getVertices());
        assertEquals(2, hiveLineage.getVertices().size());
        
        HiveLineage.Vertex vertex1 = hiveLineage.getVertices().get(0);
        assertEquals(0, vertex1.getId());
        assertEquals("COLUMN", vertex1.getVertexType());
        assertEquals("input_table.col1", vertex1.getVertexId());

        HiveLineage.Vertex vertex2 = hiveLineage.getVertices().get(1);
        assertEquals(1, vertex2.getId());
        assertEquals("COLUMN", vertex2.getVertexType());
        assertEquals("output_table.col1", vertex2.getVertexId());
    }

    /**
     * Tests successful serialization of HiveLineage object to JSON.
     * Verifies that all fields are correctly serialized.
     */
    @Test
    void testSerialization_ValidObject() {
        // Serialize HiveLineage object to JSON
        String json = gson.toJson(validHiveLineage);

        // Verify JSON is not null and contains expected fields
        assertNotNull(json);
        assertTrue(json.contains("\"version\":\"1.0\""));
        assertTrue(json.contains("\"user\":\"testuser\""));
        assertTrue(json.contains("\"timestamp\":1234567890"));
        assertTrue(json.contains("\"duration\":5000"));
        assertTrue(json.contains("\"jobIds\":[\"job1\",\"job2\"]"));
        assertTrue(json.contains("\"engine\":\"hive\""));
        assertTrue(json.contains("\"database\":\"testdb\""));
        assertTrue(json.contains("\"hash\":\"testhash123\""));
        assertTrue(json.contains("\"queryText\":\"CREATE TABLE testdb.output_table AS SELECT * FROM testdb.input_table\""));
        assertTrue(json.contains("\"environment\":\"DEV\""));
        assertTrue(json.contains("\"platformInstance\":\"urn:li:dataPlatformInstance:(urn:li:dataPlatform:hive,OPR.OCE.DEV)\""));
        assertTrue(json.contains("\"inputs\":[\"testdb.input_table\"]"));
        assertTrue(json.contains("\"outputs\":[\"testdb.output_table\"]"));
        assertTrue(json.contains("\"edgeType\":\"PROJECTION\""));
        assertTrue(json.contains("\"vertexType\":\"COLUMN\""));
    }

    /**
     * Tests round-trip serialization and deserialization.
     * Verifies that data integrity is maintained through the process.
     */
    @Test
    void testRoundTripSerialization() {
        // Serialize to JSON and then deserialize back
        String json = gson.toJson(validHiveLineage);
        HiveLineage deserializedHiveLineage = gson.fromJson(json, HiveLineage.class);

        // Verify all fields match the original
        assertEquals(validHiveLineage.getVersion(), deserializedHiveLineage.getVersion());
        assertEquals(validHiveLineage.getUser(), deserializedHiveLineage.getUser());
        assertEquals(validHiveLineage.getTimestamp(), deserializedHiveLineage.getTimestamp());
        assertEquals(validHiveLineage.getDuration(), deserializedHiveLineage.getDuration());
        assertEquals(validHiveLineage.getJobIds(), deserializedHiveLineage.getJobIds());
        assertEquals(validHiveLineage.getEngine(), deserializedHiveLineage.getEngine());
        assertEquals(validHiveLineage.getDatabase(), deserializedHiveLineage.getDatabase());
        assertEquals(validHiveLineage.getHash(), deserializedHiveLineage.getHash());
        assertEquals(validHiveLineage.getQueryText(), deserializedHiveLineage.getQueryText());
        assertEquals(validHiveLineage.getEnvironment(), deserializedHiveLineage.getEnvironment());
        assertEquals(validHiveLineage.getPlatformInstance(), deserializedHiveLineage.getPlatformInstance());
        assertEquals(validHiveLineage.getInputs(), deserializedHiveLineage.getInputs());
        assertEquals(validHiveLineage.getOutputs(), deserializedHiveLineage.getOutputs());

        // Verify edges
        assertEquals(validHiveLineage.getEdges().size(), deserializedHiveLineage.getEdges().size());
        assertEquals(validHiveLineage.getEdges().get(0).getSources(), deserializedHiveLineage.getEdges().get(0).getSources());
        assertEquals(validHiveLineage.getEdges().get(0).getTargets(), deserializedHiveLineage.getEdges().get(0).getTargets());
        assertEquals(validHiveLineage.getEdges().get(0).getExpression(), deserializedHiveLineage.getEdges().get(0).getExpression());
        assertEquals(validHiveLineage.getEdges().get(0).getEdgeType(), deserializedHiveLineage.getEdges().get(0).getEdgeType());

        // Verify vertices
        assertEquals(validHiveLineage.getVertices().size(), deserializedHiveLineage.getVertices().size());
        assertEquals(validHiveLineage.getVertices().get(0).getId(), deserializedHiveLineage.getVertices().get(0).getId());
        assertEquals(validHiveLineage.getVertices().get(0).getVertexType(), deserializedHiveLineage.getVertices().get(0).getVertexType());
        assertEquals(validHiveLineage.getVertices().get(0).getVertexId(), deserializedHiveLineage.getVertices().get(0).getVertexId());
    }

    /**
     * Tests deserialization with missing optional fields.
     * Verifies that the model handles missing fields gracefully.
     */
    @Test
    void testDeserialization_MissingOptionalFields() {
        // JSON with only required fields
        String minimalJson = "{"
            + "\"version\":\"1.0\","
            + "\"user\":\"testuser\","
            + "\"queryText\":\"SELECT * FROM table\""
            + "}";

        // Deserialize and verify
        HiveLineage hiveLineage = gson.fromJson(minimalJson, HiveLineage.class);
        
        assertNotNull(hiveLineage);
        assertEquals("1.0", hiveLineage.getVersion());
        assertEquals("testuser", hiveLineage.getUser());
        assertEquals("SELECT * FROM table", hiveLineage.getQueryText());
        
        // Optional fields should be null or default values
        assertNull(hiveLineage.getJobIds());
        assertNull(hiveLineage.getEdges());
        assertNull(hiveLineage.getVertices());
    }

    /**
     * Tests deserialization with invalid JSON.
     * Verifies that appropriate exceptions are thrown for malformed JSON.
     */
    @Test
    void testDeserialization_InvalidJson() {
        String invalidJson = "{ invalid json }";

        // Should throw JsonSyntaxException for invalid JSON
        assertThrows(JsonSyntaxException.class, () -> {
            gson.fromJson(invalidJson, HiveLineage.class);
        });
    }

    /**
     * Tests deserialization with empty JSON object.
     * Verifies that empty JSON creates a valid but empty HiveLineage object.
     */
    @Test
    void testDeserialization_EmptyJson() {
        String emptyJson = "{}";

        // Deserialize empty JSON
        HiveLineage hiveLineage = gson.fromJson(emptyJson, HiveLineage.class);
        
        // Should create a valid object with null/default values
        assertNotNull(hiveLineage);
        assertNull(hiveLineage.getVersion());
        assertNull(hiveLineage.getUser());
        assertNull(hiveLineage.getQueryText());
    }

    /**
     * Tests Edge nested class functionality.
     * Verifies that Edge objects work correctly with Lombok annotations.
     */
    @Test
    void testEdgeClass() {
        HiveLineage.Edge edge1 = new HiveLineage.Edge();
        edge1.setSources(Arrays.asList(1, 2));
        edge1.setTargets(Arrays.asList(3, 4));
        edge1.setExpression("test_expression");
        edge1.setEdgeType("DEPENDENCY");

        HiveLineage.Edge edge2 = new HiveLineage.Edge();
        edge2.setSources(Arrays.asList(1, 2));
        edge2.setTargets(Arrays.asList(3, 4));
        edge2.setExpression("test_expression");
        edge2.setEdgeType("DEPENDENCY");

        // Test getters
        assertEquals(Arrays.asList(1, 2), edge1.getSources());
        assertEquals(Arrays.asList(3, 4), edge1.getTargets());
        assertEquals("test_expression", edge1.getExpression());
        assertEquals("DEPENDENCY", edge1.getEdgeType());

        // Test Lombok generated equals and hashCode
        assertEquals(edge1, edge2);
        assertEquals(edge1.hashCode(), edge2.hashCode());

        // Test toString (Lombok generated)
        assertNotNull(edge1.toString());
        assertTrue(edge1.toString().contains("Edge"));
    }

    /**
     * Tests Vertex nested class functionality.
     * Verifies that Vertex objects work correctly with Lombok annotations.
     */
    @Test
    void testVertexClass() {
        HiveLineage.Vertex vertex1 = new HiveLineage.Vertex();
        vertex1.setId(5);
        vertex1.setVertexType("TABLE");
        vertex1.setVertexId("test_table");

        HiveLineage.Vertex vertex2 = new HiveLineage.Vertex();
        vertex2.setId(5);
        vertex2.setVertexType("TABLE");
        vertex2.setVertexId("test_table");

        // Test getters
        assertEquals(5, vertex1.getId());
        assertEquals("TABLE", vertex1.getVertexType());
        assertEquals("test_table", vertex1.getVertexId());

        // Test Lombok generated equals and hashCode
        assertEquals(vertex1, vertex2);
        assertEquals(vertex1.hashCode(), vertex2.hashCode());

        // Test toString (Lombok generated)
        assertNotNull(vertex1.toString());
        assertTrue(vertex1.toString().contains("Vertex"));
    }

    /**
     * Tests HiveLineage class Lombok functionality.
     * Verifies that Lombok annotations work correctly for the main class.
     */
    @Test
    void testHiveLineageLombokFunctionality() {
        HiveLineage hiveLineage1 = new HiveLineage();
        hiveLineage1.setVersion("2.0");
        hiveLineage1.setUser("user1");
        hiveLineage1.setEngine("spark");

        HiveLineage hiveLineage2 = new HiveLineage();
        hiveLineage2.setVersion("2.0");
        hiveLineage2.setUser("user1");
        hiveLineage2.setEngine("spark");

        // Test Lombok generated equals and hashCode
        assertEquals(hiveLineage1, hiveLineage2);
        assertEquals(hiveLineage1.hashCode(), hiveLineage2.hashCode());

        // Test toString (Lombok generated)
        assertNotNull(hiveLineage1.toString());
        assertTrue(hiveLineage1.toString().contains("HiveLineage"));
    }

    /**
     * Tests serialization with @SerializedName annotations.
     * Verifies that field names are correctly mapped in JSON.
     */
    @Test
    void testSerializedNameAnnotations() {
        HiveLineage hiveLineage = new HiveLineage();
        hiveLineage.setJobIds(Arrays.asList("job123"));
        hiveLineage.setQueryText("SELECT * FROM test");
        hiveLineage.setPlatformInstance("test-instance");

        String json = gson.toJson(hiveLineage);

        // Verify that @SerializedName annotations work correctly
        assertTrue(json.contains("\"jobIds\""));
        assertTrue(json.contains("\"queryText\""));
        assertTrue(json.contains("\"platformInstance\""));
        
        // Verify field names match the annotations
        assertFalse(json.contains("\"job_ids\""));
        assertFalse(json.contains("\"query_text\""));
        assertFalse(json.contains("\"platform_instance\""));
    }

    /**
     * Tests deserialization with complex nested structures.
     * Verifies handling of multiple edges and vertices.
     */
    @Test
    void testComplexNestedStructures() {
        String complexJson = "{"
            + "\"version\":\"1.0\","
            + "\"edges\":["
            + "{\"sources\":[0,1],\"targets\":[2],\"edgeType\":\"PROJECTION\"},"
            + "{\"sources\":[2],\"targets\":[3,4],\"edgeType\":\"DEPENDENCY\"}"
            + "],"
            + "\"vertices\":["
            + "{\"id\":0,\"vertexType\":\"COLUMN\",\"vertexId\":\"table1.col1\"},"
            + "{\"id\":1,\"vertexType\":\"COLUMN\",\"vertexId\":\"table1.col2\"},"
            + "{\"id\":2,\"vertexType\":\"COLUMN\",\"vertexId\":\"table2.col1\"},"
            + "{\"id\":3,\"vertexType\":\"TABLE\",\"vertexId\":\"table3\"},"
            + "{\"id\":4,\"vertexType\":\"TABLE\",\"vertexId\":\"table4\"}"
            + "]"
            + "}";

        HiveLineage hiveLineage = gson.fromJson(complexJson, HiveLineage.class);

        // Verify complex structures are correctly deserialized
        assertNotNull(hiveLineage.getEdges());
        assertEquals(2, hiveLineage.getEdges().size());
        
        assertNotNull(hiveLineage.getVertices());
        assertEquals(5, hiveLineage.getVertices().size());

        // Verify first edge
        HiveLineage.Edge edge1 = hiveLineage.getEdges().get(0);
        assertEquals(Arrays.asList(0, 1), edge1.getSources());
        assertEquals(Arrays.asList(2), edge1.getTargets());
        assertEquals("PROJECTION", edge1.getEdgeType());

        // Verify last vertex
        HiveLineage.Vertex lastVertex = hiveLineage.getVertices().get(4);
        assertEquals(4, lastVertex.getId());
        assertEquals("TABLE", lastVertex.getVertexType());
        assertEquals("table4", lastVertex.getVertexId());
    }
}
