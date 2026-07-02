package datahub.hive.consumer.service.impl;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.DataTemplate;
import com.linkedin.query.QueryProperties;
import com.linkedin.query.QuerySubjects;
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
 * Unit tests for QueryMCPTransformerServiceImpl.
 * Tests the transformation of query lineage JSON to MCP aspects.
 */
@ExtendWith(MockitoExtension.class)
public class QueryMCPTransformerServiceImplTest {

    @InjectMocks
    private QueryMCPTransformerServiceImpl queryMCPTransformerService;

    private JsonObject validQueryJson;

    @BeforeEach
    void setUp() {
        // Set required properties using ReflectionTestUtils
        ReflectionTestUtils.setField(queryMCPTransformerService, "environment", "DEV");
        ReflectionTestUtils.setField(queryMCPTransformerService, "serviceUser", "testuser");

        // Create a valid query JSON for testing
        validQueryJson = new JsonObject();
        validQueryJson.addProperty("version", "1.0");
        validQueryJson.addProperty("user", "testuser");
        validQueryJson.addProperty("timestamp", System.currentTimeMillis() / 1000);
        validQueryJson.addProperty("duration", 1000);
        validQueryJson.addProperty("engine", "hive");
        validQueryJson.addProperty("database", "testdb");
        validQueryJson.addProperty("hash", "testhash123");
        validQueryJson.addProperty("queryText", "CREATE TABLE testdb.output_table AS SELECT * FROM testdb.input_table");
        validQueryJson.addProperty("environment", "DEV");
        validQueryJson.addProperty("platformInstance", "urn:li:dataPlatformInstance:(urn:li:dataPlatform:hive,OPR.OCE.DEV)");
        
        // Add inputs and outputs
        validQueryJson.add("inputs", JsonParser.parseString("[\"testdb.input_table\"]").getAsJsonArray());
        validQueryJson.add("outputs", JsonParser.parseString("[\"testdb.output_table\"]").getAsJsonArray());
    }

    /**
     * Tests the main transformToMCP method with valid query JSON.
     * Verifies that all expected aspects are created.
     */
    @Test
    void testTransformToMCP_ValidQuery() throws URISyntaxException {
        // Call the method under test
        List<DataTemplate<DataMap>> aspects = queryMCPTransformerService.transformToMCP(validQueryJson);
        
        // Verify that 2 aspects are created
        assertEquals(2, aspects.size());
        
        // Verify aspect types
        assertTrue(aspects.get(0) instanceof QuerySubjects);
        assertTrue(aspects.get(1) instanceof QueryProperties);
    }

    /**
     * Tests the query subjects aspect creation.
     * Verifies that query subjects are correctly built from outputs.
     */
    @Test
    void testBuildQuerySubjectsAspect() throws URISyntaxException {
        // Call the method under test
        List<DataTemplate<DataMap>> aspects = queryMCPTransformerService.transformToMCP(validQueryJson);
        
        // Get the query subjects aspect
        QuerySubjects querySubjects = (QuerySubjects) aspects.get(0);
        
        // Verify subjects are set
        assertNotNull(querySubjects.getSubjects());
        assertFalse(querySubjects.getSubjects().isEmpty());
        assertEquals(1, querySubjects.getSubjects().size());
        
        // Verify the subject entity is set correctly
        assertNotNull(querySubjects.getSubjects().get(0).getEntity());
        assertTrue(querySubjects.getSubjects().get(0).getEntity().toString().contains("output_table"));
    }

    /**
     * Tests the query properties aspect creation.
     * Verifies that query properties are correctly built.
     */
    @Test
    void testBuildQueryPropertiesAspect() throws URISyntaxException {
        // Call the method under test
        List<DataTemplate<DataMap>> aspects = queryMCPTransformerService.transformToMCP(validQueryJson);
        
        // Get the query properties aspect
        QueryProperties queryProperties = (QueryProperties) aspects.get(1);
        
        // Verify statement is set
        assertNotNull(queryProperties.getStatement());
        assertEquals("CREATE TABLE testdb.output_table AS SELECT * FROM testdb.input_table", 
                    queryProperties.getStatement().getValue());
        assertEquals("SQL", queryProperties.getStatement().getLanguage().name());
        
        // Verify timestamps are set
        assertNotNull(queryProperties.getCreated());
        assertNotNull(queryProperties.getLastModified());
        assertTrue(queryProperties.getCreated().getTime() > 0);
        assertTrue(queryProperties.getLastModified().getTime() > 0);
        
        // Verify actor is set
        assertNotNull(queryProperties.getCreated().getActor());
        assertNotNull(queryProperties.getLastModified().getActor());
        assertTrue(queryProperties.getCreated().getActor().toString().contains("testuser"));
        assertTrue(queryProperties.getLastModified().getActor().toString().contains("testuser"));
        
        // Verify source is set to MANUAL
        assertEquals("MANUAL", queryProperties.getSource().name());
    }

    /**
     * Tests transformToMCP with query JSON without outputs.
     * Verifies handling of missing outputs.
     */
    @Test
    void testTransformToMCP_NoOutputs() throws URISyntaxException {
        // Remove outputs from the query JSON
        validQueryJson.remove("outputs");
        
        // Call the method under test
        List<DataTemplate<DataMap>> aspects = queryMCPTransformerService.transformToMCP(validQueryJson);
        
        // Verify that aspects are still created
        assertEquals(2, aspects.size());
        
        // Verify query subjects aspect has no subjects
        QuerySubjects querySubjects = (QuerySubjects) aspects.get(0);
        if (querySubjects.hasSubjects()) {
            assertTrue(querySubjects.getSubjects().isEmpty());
        }
    }

    /**
     * Tests transformToMCP with empty outputs array.
     * Verifies handling of empty outputs.
     */
    @Test
    void testTransformToMCP_EmptyOutputs() throws URISyntaxException {
        // Set empty outputs array
        validQueryJson.add("outputs", JsonParser.parseString("[]").getAsJsonArray());
        
        // Call the method under test
        List<DataTemplate<DataMap>> aspects = queryMCPTransformerService.transformToMCP(validQueryJson);
        
        // Verify that aspects are still created
        assertEquals(2, aspects.size());
        
        // Verify query subjects aspect has no subjects
        QuerySubjects querySubjects = (QuerySubjects) aspects.get(0);
        if (querySubjects.hasSubjects()) {
            assertTrue(querySubjects.getSubjects().isEmpty());
        }
    }

    /**
     * Tests transformToMCP with multiple outputs.
     * Verifies handling of multiple output datasets.
     */
    @Test
    void testTransformToMCP_MultipleOutputs() throws URISyntaxException {
        // Add multiple outputs
        validQueryJson.add("outputs", JsonParser.parseString("[\"testdb.output_table1\", \"testdb.output_table2\"]").getAsJsonArray());
        
        // Call the method under test
        List<DataTemplate<DataMap>> aspects = queryMCPTransformerService.transformToMCP(validQueryJson);
        
        // Verify that aspects are created
        assertEquals(2, aspects.size());
        
        // Verify query subjects aspect - should only use the first output
        QuerySubjects querySubjects = (QuerySubjects) aspects.get(0);
        assertNotNull(querySubjects.getSubjects());
        assertEquals(1, querySubjects.getSubjects().size());
        assertTrue(querySubjects.getSubjects().get(0).getEntity().toString().contains("output_table1"));
    }

    /**
     * Tests transformToMCP with different query text.
     * Verifies handling of various SQL statements.
     */
    @Test
    void testTransformToMCP_DifferentQueryText() throws URISyntaxException {
        // Change query text to a SELECT statement
        validQueryJson.addProperty("queryText", "SELECT col1, col2 FROM testdb.input_table WHERE col1 > 100");
        
        // Call the method under test
        List<DataTemplate<DataMap>> aspects = queryMCPTransformerService.transformToMCP(validQueryJson);
        
        // Verify that aspects are created
        assertEquals(2, aspects.size());
        
        // Verify query properties aspect has the correct query text
        QueryProperties queryProperties = (QueryProperties) aspects.get(1);
        assertEquals("SELECT col1, col2 FROM testdb.input_table WHERE col1 > 100", 
                    queryProperties.getStatement().getValue());
    }

    /**
     * Tests transformToMCP with missing queryText field.
     * Verifies handling of missing query text.
     */
    @Test
    void testTransformToMCP_MissingQueryText() {
        // Remove queryText field
        validQueryJson.remove("queryText");
        
        // Call the method under test - should throw exception due to missing queryText
        assertThrows(Exception.class, () -> {
            queryMCPTransformerService.transformToMCP(validQueryJson);
        });
    }

    /**
     * Tests transformToMCP with null queryText field.
     * Verifies handling of null query text.
     */
    @Test
    void testTransformToMCP_NullQueryText() {
        // Set queryText to null
        validQueryJson.add("queryText", null);
        
        // Call the method under test - should throw exception due to null queryText
        assertThrows(Exception.class, () -> {
            queryMCPTransformerService.transformToMCP(validQueryJson);
        });
    }

    /**
     * Tests transformToMCP with missing platformInstance field.
     * Verifies handling of missing platform instance.
     */
    @Test
    void testTransformToMCP_MissingPlatformInstance() {
        // Remove platformInstance field
        validQueryJson.remove("platformInstance");
        
        // Call the method under test - should throw exception due to missing platformInstance
        assertThrows(Exception.class, () -> {
            queryMCPTransformerService.transformToMCP(validQueryJson);
        });
    }

    /**
     * Tests query subjects aspect with complex dataset names.
     * Verifies handling of dataset names with special characters.
     */
    @Test
    void testBuildQuerySubjectsAspect_ComplexDatasetNames() throws URISyntaxException {
        // Set output with complex name
        validQueryJson.add("outputs", JsonParser.parseString("[\"test_db.complex_table_name_123\"]").getAsJsonArray());
        
        // Call the method under test
        List<DataTemplate<DataMap>> aspects = queryMCPTransformerService.transformToMCP(validQueryJson);
        
        // Get the query subjects aspect
        QuerySubjects querySubjects = (QuerySubjects) aspects.get(0);
        
        // Verify subjects are set correctly
        assertNotNull(querySubjects.getSubjects());
        assertEquals(1, querySubjects.getSubjects().size());
        assertTrue(querySubjects.getSubjects().get(0).getEntity().toString().contains("complex_table_name_123"));
    }

    /**
     * Tests query properties aspect with long query text.
     * Verifies handling of large SQL statements.
     */
    @Test
    void testBuildQueryPropertiesAspect_LongQueryText() throws URISyntaxException {
        // Create a long query text
        StringBuilder longQuery = new StringBuilder();
        longQuery.append("SELECT ");
        for (int i = 1; i <= 50; i++) {
            longQuery.append("col").append(i);
            if (i < 50) longQuery.append(", ");
        }
        longQuery.append(" FROM testdb.large_table WHERE col1 > 0 AND col2 < 1000");
        
        validQueryJson.addProperty("queryText", longQuery.toString());
        
        // Call the method under test
        List<DataTemplate<DataMap>> aspects = queryMCPTransformerService.transformToMCP(validQueryJson);
        
        // Get the query properties aspect
        QueryProperties queryProperties = (QueryProperties) aspects.get(1);
        
        // Verify the long query text is preserved
        assertEquals(longQuery.toString(), queryProperties.getStatement().getValue());
        assertTrue(queryProperties.getStatement().getValue().length() > 200);
    }
}
