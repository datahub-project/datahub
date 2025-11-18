package datahub.hive.consumer.service;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.DataTemplate;
import datahub.client.kafka.KafkaEmitter;
import datahub.hive.consumer.util.MCPEmitterUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class LineageConsumerMCPEmitterServiceTest {

    @Mock
    private MCPTransformerService datasetMCPTransformerService;

    @Mock
    private MCPTransformerService queryMCPTransformerService;

    @Mock
    private KafkaEmitter kafkaEmitter;

    private LineageConsumerMCPEmitterService lineageConsumerMCPEmitterService;

    private String validLineageMessage;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        lineageConsumerMCPEmitterService = new LineageConsumerMCPEmitterService(
            datasetMCPTransformerService, queryMCPTransformerService, kafkaEmitter);
        // Create a valid lineage message for testing
        JsonObject lineageJson = new JsonObject();
        lineageJson.addProperty("version", "1.0");
        lineageJson.addProperty("user", "testuser");
        lineageJson.addProperty("timestamp", System.currentTimeMillis() / 1000);
        lineageJson.addProperty("duration", 1000);
        lineageJson.addProperty("engine", "hive");
        lineageJson.addProperty("database", "testdb");
        lineageJson.addProperty("hash", "testhash");
        lineageJson.addProperty("queryText", "CREATE TABLE testdb.output_table AS SELECT * FROM testdb.input_table");
        lineageJson.addProperty("environment", "DEV");
        lineageJson.addProperty("platformInstance", "urn:li:dataPlatformInstance:(urn:li:dataPlatform:hive,OPR.OCE.DEV)");
        
        // Add inputs and outputs
        JsonObject inputs = new JsonObject();
        inputs.add("inputs", JsonParser.parseString("[\"testdb.input_table\"]").getAsJsonArray());
        lineageJson.add("inputs", JsonParser.parseString("[\"testdb.input_table\"]").getAsJsonArray());
        
        JsonObject outputs = new JsonObject();
        outputs.add("outputs", JsonParser.parseString("[\"testdb.output_table\"]").getAsJsonArray());
        lineageJson.add("outputs", JsonParser.parseString("[\"testdb.output_table\"]").getAsJsonArray());
        
        validLineageMessage = lineageJson.toString();
    }

    /**
     * Tests the successful consumption and processing of a valid lineage message.
     * Verifies that both transformer services are called and MCPs are emitted.
     */
    @Test
    void testConsumeLineageMessage() throws Exception {
        // Create a consumer record with the valid lineage message
        ConsumerRecord<String, String> record = new ConsumerRecord<>("HiveLineage_v1", 0, 0, "testkey", validLineageMessage);
        
        // Create mock aspects
        List<DataTemplate<DataMap>> datasetAspects = new ArrayList<>();
        DataTemplate<DataMap> datasetAspect = mock(DataTemplate.class);
        datasetAspects.add(datasetAspect);
        
        List<DataTemplate<DataMap>> queryAspects = new ArrayList<>();
        DataTemplate<DataMap> queryAspect = mock(DataTemplate.class);
        queryAspects.add(queryAspect);
        
        // Mock the transformer services to return aspects
        when(datasetMCPTransformerService.transformToMCP(any())).thenReturn(datasetAspects);
        when(queryMCPTransformerService.transformToMCP(any())).thenReturn(queryAspects);
        
        // Use try-with-resources for MockedStatic to ensure it's properly closed
        try (MockedStatic<MCPEmitterUtil> mockedStatic = mockStatic(MCPEmitterUtil.class)) {
            // Mock the MCPEmitterUtil to not throw exceptions
            mockedStatic.when(() -> MCPEmitterUtil.emitMCP(any(), any(), any(), any()))
                .thenAnswer(invocation -> null); // void method, so return null
            
            // Call the method under test
            lineageConsumerMCPEmitterService.consumeLineageMessage(record);
            
            // Verify that the transformer services were called
            verify(datasetMCPTransformerService).transformToMCP(any());
            verify(queryMCPTransformerService).transformToMCP(any());
            
            // Verify that MCPEmitterUtil.emitMCP was called for each aspect
            mockedStatic.verify(() -> MCPEmitterUtil.emitMCP(any(), any(), any(), eq(kafkaEmitter)), times(2));
        }
    }

    /**
     * Tests handling of invalid JSON in the lineage message.
     * Verifies that an exception is thrown and no transformer services are called.
     */
    @Test
    void testConsumeLineageMessageWithInvalidJson() throws Exception {
        // Create a consumer record with invalid JSON
        ConsumerRecord<String, String> record = new ConsumerRecord<>("HiveLineage_v1", 0, 0, "testkey", "invalid json");
        
        try {
            // Call the method under test - should throw an exception for invalid JSON
            lineageConsumerMCPEmitterService.consumeLineageMessage(record);
            fail("Expected an exception for invalid JSON");
        } catch (Exception e) {
            // Expected exception
        }
        
        // Verify that the transformer services were not called
        verifyNoInteractions(datasetMCPTransformerService, queryMCPTransformerService);
    }

    /**
     * Tests handling of lineage messages with no outputs.
     * Verifies that such messages are skipped and no transformer services are called.
     */
    @Test
    void testConsumeLineageMessageWithNoOutputs() throws Exception {
        // Create a lineage message with no outputs
        JsonObject lineageJson = new JsonObject();
        lineageJson.addProperty("version", "1.0");
        lineageJson.addProperty("queryText", "SELECT * FROM testdb.input_table");
        lineageJson.add("inputs", JsonParser.parseString("[\"testdb.input_table\"]").getAsJsonArray());
        // No outputs
        
        // Create a consumer record with the lineage message
        ConsumerRecord<String, String> record = new ConsumerRecord<>("HiveLineage_v1", 0, 0, "testkey", lineageJson.toString());
        
        // Call the method under test
        lineageConsumerMCPEmitterService.consumeLineageMessage(record);
        
        // Verify that the transformer services were not called
        verifyNoInteractions(datasetMCPTransformerService, queryMCPTransformerService);
    }
}
