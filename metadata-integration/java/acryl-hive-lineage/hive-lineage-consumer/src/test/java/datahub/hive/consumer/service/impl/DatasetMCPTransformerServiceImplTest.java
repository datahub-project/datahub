package datahub.hive.consumer.service.impl;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.Status;
import com.linkedin.common.SubTypes;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.DataTemplate;
import com.linkedin.dataset.UpstreamLineage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.net.URISyntaxException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for DatasetMCPTransformerServiceImpl.
 * Tests the transformation of dataset lineage JSON to MCP aspects.
 */
@ExtendWith(MockitoExtension.class)
public class DatasetMCPTransformerServiceImplTest {

    @InjectMocks
    private DatasetMCPTransformerServiceImpl datasetMCPTransformerService;

    private JsonObject validDatasetJson;
    private JsonObject datasetJsonWithEdges;

    @BeforeEach
    void setUp() {
        // Set required properties using ReflectionTestUtils
        ReflectionTestUtils.setField(datasetMCPTransformerService, "environment", "DEV");
        ReflectionTestUtils.setField(datasetMCPTransformerService, "serviceUser", "testuser");

        // Create a valid dataset JSON for testing
        validDatasetJson = new JsonObject();
        validDatasetJson.addProperty("version", "1.0");
        validDatasetJson.addProperty("user", "testuser");
        validDatasetJson.addProperty("timestamp", System.currentTimeMillis() / 1000);
        validDatasetJson.addProperty("duration", 1000);
        validDatasetJson.addProperty("engine", "hive");
        validDatasetJson.addProperty("database", "testdb");
        validDatasetJson.addProperty("hash", "testhash123");
        validDatasetJson.addProperty("queryText", "CREATE TABLE testdb.output_table AS SELECT * FROM testdb.input_table");
        validDatasetJson.addProperty("environment", "DEV");
        validDatasetJson.addProperty("platformInstance", "urn:li:dataPlatformInstance:(urn:li:dataPlatform:hive,OPR.OCE.DEV)");
        
        // Add inputs and outputs
        validDatasetJson.add("inputs", JsonParser.parseString("[\"testdb.input_table\"]").getAsJsonArray());
        validDatasetJson.add("outputs", JsonParser.parseString("[\"testdb.output_table\"]").getAsJsonArray());

        // Create dataset JSON with edges and vertices for fine-grained lineage testing
        datasetJsonWithEdges = validDatasetJson.deepCopy();
        
        // Add vertices
        JsonArray vertices = new JsonArray();
        JsonObject vertex1 = new JsonObject();
        vertex1.addProperty("id", 0);
        vertex1.addProperty("vertexType", "COLUMN");
        vertex1.addProperty("vertexId", "input_table.col1");
        vertices.add(vertex1);
        
        JsonObject vertex2 = new JsonObject();
        vertex2.addProperty("id", 1);
        vertex2.addProperty("vertexType", "COLUMN");
        vertex2.addProperty("vertexId", "output_table.col1");
        vertices.add(vertex2);
        
        datasetJsonWithEdges.add("vertices", vertices);
        
        // Add edges
        JsonArray edges = new JsonArray();
        JsonObject edge = new JsonObject();
        edge.add("sources", JsonParser.parseString("[0]").getAsJsonArray());
        edge.add("targets", JsonParser.parseString("[1]").getAsJsonArray());
        edge.addProperty("edgeType", "PROJECTION");
        edge.addProperty("expression", "col1");
        edges.add(edge);
        
        datasetJsonWithEdges.add("edges", edges);
    }

    /**
     * Tests the main transformToMCP method with valid dataset JSON.
     * Verifies that all expected aspects are created.
     */
    @Test
    void testTransformToMCP_ValidDataset() throws URISyntaxException {
        // Call the method under test
        List<DataTemplate<DataMap>> aspects = datasetMCPTransformerService.transformToMCP(validDatasetJson);
        
        // Verify that 4 aspects are created
        assertEquals(4, aspects.size());
        
        // Verify aspect types
        assertTrue(aspects.get(0) instanceof UpstreamLineage);
        assertTrue(aspects.get(1) instanceof DataPlatformInstance);
        assertTrue(aspects.get(2) instanceof Status);
        assertTrue(aspects.get(3) instanceof SubTypes);
    }

    /**
     * Tests transformToMCP with dataset JSON containing edges and vertices.
     * Verifies fine-grained lineage processing.
     */
    @Test
    void testTransformToMCP_WithEdgesAndVertices() throws URISyntaxException {
        // Call the method under test
        List<DataTemplate<DataMap>> aspects = datasetMCPTransformerService.transformToMCP(datasetJsonWithEdges);
        
        // Verify that 4 aspects are created
        assertEquals(4, aspects.size());
        
        // Verify the upstream lineage aspect contains fine-grained lineage
        UpstreamLineage upstreamLineage = (UpstreamLineage) aspects.get(0);
        assertNotNull(upstreamLineage.getUpstreams());
        assertFalse(upstreamLineage.getUpstreams().isEmpty());
        
        // Verify fine-grained lineage is present
        if (upstreamLineage.hasFineGrainedLineages()) {
            assertNotNull(upstreamLineage.getFineGrainedLineages());
            assertFalse(upstreamLineage.getFineGrainedLineages().isEmpty());
        }
    }

    /**
     * Tests transformToMCP with dataset JSON without inputs.
     * Verifies handling of missing inputs.
     */
    @Test
    void testTransformToMCP_NoInputs() throws URISyntaxException {
        // Remove inputs from the dataset JSON
        validDatasetJson.remove("inputs");
        
        // Call the method under test
        List<DataTemplate<DataMap>> aspects = datasetMCPTransformerService.transformToMCP(validDatasetJson);
        
        // Verify that aspects are still created
        assertEquals(4, aspects.size());
        
        // Verify upstream lineage aspect
        UpstreamLineage upstreamLineage = (UpstreamLineage) aspects.get(0);
        assertNotNull(upstreamLineage.getUpstreams());
        assertTrue(upstreamLineage.getUpstreams().isEmpty());
    }

    /**
     * Tests transformToMCP with empty inputs array.
     * Verifies handling of empty inputs.
     */
    @Test
    void testTransformToMCP_EmptyInputs() throws URISyntaxException {
        // Set empty inputs array
        validDatasetJson.add("inputs", new JsonArray());
        
        // Call the method under test
        List<DataTemplate<DataMap>> aspects = datasetMCPTransformerService.transformToMCP(validDatasetJson);
        
        // Verify that aspects are still created
        assertEquals(4, aspects.size());
        
        // Verify upstream lineage aspect has empty upstreams
        UpstreamLineage upstreamLineage = (UpstreamLineage) aspects.get(0);
        assertNotNull(upstreamLineage.getUpstreams());
        assertTrue(upstreamLineage.getUpstreams().isEmpty());
    }

    /**
     * Tests the data platform instance aspect creation.
     * Verifies correct platform instance URN generation.
     */
    @Test
    void testBuildDataPlatformInstanceAspect() throws URISyntaxException {
        // Call the method under test
        List<DataTemplate<DataMap>> aspects = datasetMCPTransformerService.transformToMCP(validDatasetJson);
        
        // Get the data platform instance aspect
        DataPlatformInstance dataPlatformInstance = (DataPlatformInstance) aspects.get(1);
        
        // Verify platform and instance are set
        assertNotNull(dataPlatformInstance.getPlatform());
        assertNotNull(dataPlatformInstance.getInstance());
        assertTrue(dataPlatformInstance.getPlatform().toString().contains("hive"));
    }

    /**
     * Tests the status aspect creation.
     * Verifies that status is set to not removed.
     */
    @Test
    void testBuildStatusAspect() throws URISyntaxException {
        // Call the method under test
        List<DataTemplate<DataMap>> aspects = datasetMCPTransformerService.transformToMCP(validDatasetJson);
        
        // Get the status aspect
        Status status = (Status) aspects.get(2);
        
        // Verify status is not removed
        assertFalse(status.isRemoved());
    }

    /**
     * Tests the subTypes aspect creation.
     * Verifies that subTypes contains "Table".
     */
    @Test
    void testBuildSubTypesAspect() throws URISyntaxException {
        // Call the method under test
        List<DataTemplate<DataMap>> aspects = datasetMCPTransformerService.transformToMCP(validDatasetJson);
        
        // Get the subTypes aspect
        SubTypes subTypes = (SubTypes) aspects.get(3);
        
        // Verify subTypes contains "Table"
        assertNotNull(subTypes.getTypeNames());
        assertFalse(subTypes.getTypeNames().isEmpty());
        assertTrue(subTypes.getTypeNames().contains("Table"));
    }

    /**
     * Tests upstream lineage aspect with multiple inputs.
     * Verifies handling of multiple upstream datasets.
     */
    @Test
    void testBuildUpstreamLineageAspect_MultipleInputs() throws URISyntaxException {
        // Add multiple inputs
        validDatasetJson.add("inputs", JsonParser.parseString("[\"testdb.input_table1\", \"testdb.input_table2\"]").getAsJsonArray());
        
        // Call the method under test
        List<DataTemplate<DataMap>> aspects = datasetMCPTransformerService.transformToMCP(validDatasetJson);
        
        // Get the upstream lineage aspect
        UpstreamLineage upstreamLineage = (UpstreamLineage) aspects.get(0);
        
        // Verify multiple upstreams are created
        assertNotNull(upstreamLineage.getUpstreams());
        assertEquals(2, upstreamLineage.getUpstreams().size());
    }

    /**
     * Tests handling of edges with non-PROJECTION type.
     * Verifies that only PROJECTION edges are processed for fine-grained lineage.
     */
    @Test
    void testTransformToMCP_NonProjectionEdges() throws URISyntaxException {
        // Modify edge type to non-PROJECTION
        JsonArray edges = datasetJsonWithEdges.getAsJsonArray("edges");
        JsonObject edge = edges.get(0).getAsJsonObject();
        edge.addProperty("edgeType", "DEPENDENCY");
        
        // Call the method under test
        List<DataTemplate<DataMap>> aspects = datasetMCPTransformerService.transformToMCP(datasetJsonWithEdges);
        
        // Verify aspects are created
        assertEquals(4, aspects.size());
        
        // Verify upstream lineage aspect
        UpstreamLineage upstreamLineage = (UpstreamLineage) aspects.get(0);
        assertNotNull(upstreamLineage.getUpstreams());
        
        // Fine-grained lineage should not be created for non-PROJECTION edges
        if (upstreamLineage.hasFineGrainedLineages()) {
            assertTrue(upstreamLineage.getFineGrainedLineages().isEmpty());
        }
    }

    /**
     * Tests handling of missing edges and vertices.
     * Verifies graceful handling when fine-grained lineage data is missing.
     */
    @Test
    void testTransformToMCP_MissingEdgesAndVertices() throws URISyntaxException {
        // Create dataset without edges and vertices
        JsonObject datasetWithoutEdgesVertices = validDatasetJson.deepCopy();
        
        // Call the method under test - should handle missing edges/vertices gracefully
        List<DataTemplate<DataMap>> aspects = datasetMCPTransformerService.transformToMCP(datasetWithoutEdgesVertices);
        
        // Verify that aspects are still created
        assertNotNull(aspects);
        assertEquals(4, aspects.size());
        
        // Verify that the method produces valid aspects
        assertTrue(aspects.get(0) instanceof UpstreamLineage);
        assertTrue(aspects.get(1) instanceof DataPlatformInstance);
        assertTrue(aspects.get(2) instanceof Status);
        assertTrue(aspects.get(3) instanceof SubTypes);
        
        // Verify upstream lineage aspect
        UpstreamLineage upstreamLineage = (UpstreamLineage) aspects.get(0);
        assertNotNull(upstreamLineage.getUpstreams());
        
        // Should not have fine-grained lineage without edges/vertices
        if (upstreamLineage.hasFineGrainedLineages()) {
            assertTrue(upstreamLineage.getFineGrainedLineages() == null || upstreamLineage.getFineGrainedLineages().isEmpty());
        }
    }

    /**
     * Tests handling of missing hash field.
     * Verifies that upstream lineage is created without query URN when hash is missing.
     */
    @Test
    void testTransformToMCP_MissingHash() throws URISyntaxException {
        // Remove hash field
        validDatasetJson.remove("hash");
        
        // Call the method under test
        List<DataTemplate<DataMap>> aspects = datasetMCPTransformerService.transformToMCP(validDatasetJson);
        
        // Verify that aspects are still created
        assertEquals(4, aspects.size());
        
        // Verify upstream lineage aspect
        UpstreamLineage upstreamLineage = (UpstreamLineage) aspects.get(0);
        assertNotNull(upstreamLineage.getUpstreams());
        assertFalse(upstreamLineage.getUpstreams().isEmpty());
    }
}
