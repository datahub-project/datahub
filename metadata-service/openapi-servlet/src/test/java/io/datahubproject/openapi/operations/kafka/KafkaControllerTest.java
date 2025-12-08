package io.datahubproject.openapi.operations.kafka;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.testng.Assert.assertNotNull;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.trace.MCLTraceReader;
import com.linkedin.metadata.trace.MCPTraceReader;
import io.datahubproject.metadata.context.ObjectMapperContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.openapi.config.TracingInterceptor;
import io.datahubproject.openapi.operations.kafka.models.KafkaClusterResponse;
import io.datahubproject.openapi.operations.kafka.models.KafkaTopicResponse;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SpringBootTest(classes = KafkaControllerTest.TestConfig.class)
@AutoConfigureWebMvc
@AutoConfigureMockMvc
public class KafkaControllerTest extends AbstractTestNGSpringContextTests {

  private static final String MCP_CONSUMER_GROUP = "datahub-mcp-consumer";
  private static final String MCL_CONSUMER_GROUP = "datahub-mcl-consumer";
  private static final String MCL_TIMESERIES_CONSUMER_GROUP = "datahub-mcl-timeseries-consumer";
  private static final String TOPIC_1 = "MetadataChangeProposal_v1";
  private static final String TOPIC_2 = "MetadataChangeLog_v1";
  private static final String TOPIC_3 = "MetadataChangeLog_Timeseries_v1";

  @Autowired private KafkaController kafkaController;

  @Autowired private MockMvc mockMvc;

  @Autowired private MCPTraceReader mockMcpTraceReader;

  @Autowired
  @Qualifier("mclVersionedTraceReader")
  private MCLTraceReader mockMclTraceReader;

  @Autowired
  @Qualifier("mclTimeseriesTraceReader")
  private MCLTraceReader mockMclTimeseriesTraceReader;

  @Autowired private AuthorizerChain authorizerChain;

  @Autowired
  @Qualifier("kafkaAdminService")
  private KafkaAdminService kafkaAdminService;

  private Map<TopicPartition, OffsetAndMetadata> createOffsetMap(String topic) {
    Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
    // Create 3 partitions for the topic
    offsetMap.put(new TopicPartition(topic, 0), new OffsetAndMetadata(1000L, "metadata-p0"));
    offsetMap.put(new TopicPartition(topic, 1), new OffsetAndMetadata(2000L, "metadata-p1"));
    offsetMap.put(new TopicPartition(topic, 2), new OffsetAndMetadata(3000L, "metadata-p2"));
    return offsetMap;
  }

  private Map<TopicPartition, Long> createEndOffsetMap(String topic) {
    Map<TopicPartition, Long> endOffsetMap = new HashMap<>();
    // Create end offsets with some lag
    endOffsetMap.put(new TopicPartition(topic, 0), 1100L); // 100 lag
    endOffsetMap.put(new TopicPartition(topic, 1), 2500L); // 500 lag
    endOffsetMap.put(new TopicPartition(topic, 2), 3050L); // 50 lag
    return endOffsetMap;
  }

  @BeforeMethod
  public void setupMocks() {
    // Setup MCP reader
    when(mockMcpTraceReader.getConsumerGroupId()).thenReturn(MCP_CONSUMER_GROUP);
    when(mockMcpTraceReader.getAllPartitionOffsets(anyBoolean()))
        .thenReturn(createOffsetMap(TOPIC_1));
    when(mockMcpTraceReader.getEndOffsets(anyCollection(), anyBoolean()))
        .thenAnswer(
            invocation -> {
              Collection<TopicPartition> partitions = invocation.getArgument(0);
              return createEndOffsetMap(TOPIC_1);
            });

    // Setup MCL reader
    when(mockMclTraceReader.getConsumerGroupId()).thenReturn(MCL_CONSUMER_GROUP);
    when(mockMclTraceReader.getAllPartitionOffsets(anyBoolean()))
        .thenReturn(createOffsetMap(TOPIC_2));
    when(mockMclTraceReader.getEndOffsets(anyCollection(), anyBoolean()))
        .thenAnswer(
            invocation -> {
              Collection<TopicPartition> partitions = invocation.getArgument(0);
              return createEndOffsetMap(TOPIC_2);
            });

    // Setup MCL Timeseries reader
    when(mockMclTimeseriesTraceReader.getConsumerGroupId())
        .thenReturn(MCL_TIMESERIES_CONSUMER_GROUP);
    when(mockMclTimeseriesTraceReader.getAllPartitionOffsets(anyBoolean()))
        .thenReturn(createOffsetMap(TOPIC_3));
    when(mockMclTimeseriesTraceReader.getEndOffsets(anyCollection(), anyBoolean()))
        .thenAnswer(
            invocation -> {
              Collection<TopicPartition> partitions = invocation.getArgument(0);
              return createEndOffsetMap(TOPIC_3);
            });

    // Setup AuthorizationContext
    Authentication authentication = mock(Authentication.class);
    when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "datahub"));
    AuthenticationContext.setAuthentication(authentication);

    // Setup AuthorizerChain to allow access
    when(authorizerChain.authorize(any()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
  }

  @Test
  public void initTest() {
    assertNotNull(kafkaController);
  }

  @Test
  public void testGetMCPOffsets() throws Exception {
    // Test the MCP offsets endpoint
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/kafka/mcp/consumer/offsets")
                .param("skipCache", "false")
                .param("detailed", "true")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andDo(
            result -> {
              String responseContent = result.getResponse().getContentAsString();
              System.out.println("Response content: " + responseContent);
            })
        // Verify consumer group id
        .andExpect(MockMvcResultMatchers.jsonPath("$.consumerGroupId").value(MCP_CONSUMER_GROUP))
        // Verify topic
        .andExpect(MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_1 + "']").exists())
        // Verify partitions
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_1 + "'].partitions['0'].offset")
                .value(1000))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_1 + "'].partitions['0'].metadata")
                .value("metadata-p0"))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_1 + "'].partitions['0'].lag")
                .value(100))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_1 + "'].partitions['1'].offset")
                .value(2000))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_1 + "'].partitions['1'].lag")
                .value(500))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_1 + "'].partitions['2'].offset")
                .value(3000))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_1 + "'].partitions['2'].lag")
                .value(50))
        // Verify metrics
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_1 + "'].metrics.maxLag").value(500))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_1 + "'].metrics.medianLag")
                .value(100))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_1 + "'].metrics.totalLag")
                .value(650))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_1 + "'].metrics.avgLag")
                .value(217));
  }

  @Test
  public void testGetMCPOffsetsWithoutDetails() throws Exception {
    // Test the MCP offsets endpoint without detailed info
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/kafka/mcp/consumer/offsets")
                .param("skipCache", "false")
                .param("detailed", "false")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andDo(
            result -> {
              String responseContent = result.getResponse().getContentAsString();
              System.out.println("Response content: " + responseContent);
            })
        // Verify consumer group id and topic
        .andExpect(MockMvcResultMatchers.jsonPath("$.consumerGroupId").value(MCP_CONSUMER_GROUP))
        .andExpect(MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_1 + "']").exists())
        // Partitions should not be included
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_1 + "'].partitions").doesNotExist())
        // Metrics should still be included
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_1 + "'].metrics.maxLag").value(500))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_1 + "'].metrics.totalLag")
                .value(650));
  }

  @Test
  public void testGetMCLOffsets() throws Exception {
    // Test the MCL offsets endpoint
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/kafka/mcl/consumer/offsets")
                .param("skipCache", "true")
                .param("detailed", "true")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andDo(
            result -> {
              String responseContent = result.getResponse().getContentAsString();
              System.out.println("Response content: " + responseContent);
            })
        // Verify consumer group id
        .andExpect(MockMvcResultMatchers.jsonPath("$.consumerGroupId").value(MCL_CONSUMER_GROUP))
        // Verify topic
        .andExpect(MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_2 + "']").exists())
        // Verify partitions
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_2 + "'].partitions['0'].offset")
                .value(1000))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_2 + "'].partitions['1'].offset")
                .value(2000))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_2 + "'].partitions['2'].offset")
                .value(3000))
        // Verify metrics
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_2 + "'].metrics.maxLag").value(500))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_2 + "'].metrics.totalLag")
                .value(650));
  }

  @Test
  public void testGetMCLTimeseriesOffsets() throws Exception {
    // Test the MCL timeseries offsets endpoint
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/kafka/mcl-timeseries/consumer/offsets")
                .param("skipCache", "false")
                .param("detailed", "true")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andDo(
            result -> {
              String responseContent = result.getResponse().getContentAsString();
              System.out.println("Response content: " + responseContent);
            })
        // Verify consumer group id
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.consumerGroupId")
                .value(MCL_TIMESERIES_CONSUMER_GROUP))
        // Verify topic
        .andExpect(MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_3 + "']").exists())
        // Verify partitions
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_3 + "'].partitions['0'].offset")
                .value(1000))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_3 + "'].partitions['1'].offset")
                .value(2000))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_3 + "'].partitions['2'].offset")
                .value(3000))
        // Verify metrics
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_3 + "'].metrics.maxLag").value(500))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.topics['" + TOPIC_3 + "'].metrics.totalLag")
                .value(650));
  }

  @Test
  public void testEmptyOffsets() throws Exception {
    // Setup MCP reader to return empty offset map for this test
    when(mockMcpTraceReader.getAllPartitionOffsets(anyBoolean())).thenReturn(new HashMap<>());

    // Test the MCP offsets endpoint with empty results
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/kafka/mcp/consumer/offsets")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        // Should return a response with consumer group id but empty topics
        .andExpect(MockMvcResultMatchers.jsonPath("$.consumerGroupId").value(MCP_CONSUMER_GROUP))
        .andExpect(MockMvcResultMatchers.jsonPath("$.topics").isEmpty());
  }

  // ============================================================================
  // Cluster Info Tests
  // ============================================================================

  @Test
  public void testGetClusterInfo() throws Exception {
    // Setup mock
    KafkaClusterResponse clusterInfo =
        KafkaClusterResponse.builder()
            .clusterId("test-cluster-id")
            .controller(
                KafkaClusterResponse.BrokerInfo.builder()
                    .id(0)
                    .host("localhost")
                    .port(9092)
                    .rack("rack1")
                    .build())
            .brokers(java.util.List.of())
            .build();
    when(kafkaAdminService.getClusterInfo()).thenReturn(clusterInfo);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/kafka/cluster")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$.clusterId").value("test-cluster-id"));
  }

  @Test
  public void testGetClusterInfo_Error() throws Exception {
    // Setup mock to throw exception
    when(kafkaAdminService.getClusterInfo())
        .thenThrow(new KafkaAdminException("Failed to connect to Kafka"));

    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/kafka/cluster")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isInternalServerError())
        .andExpect(MockMvcResultMatchers.jsonPath("$.error").exists());
  }

  // ============================================================================
  // Topic Tests
  // ============================================================================

  @Test
  public void testListTopics() throws Exception {
    when(kafkaAdminService.listTopics(false))
        .thenReturn(java.util.List.of("MetadataChangeProposal_v1", "MetadataChangeLog_v1"));

    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/kafka/topics")
                .param("includeInternal", "false")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$.count").value(2))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.topics[0]").value("MetadataChangeProposal_v1"));
  }

  @Test
  public void testListTopics_Error() throws Exception {
    when(kafkaAdminService.listTopics(anyBoolean()))
        .thenThrow(new KafkaAdminException("Failed to list topics"));

    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/kafka/topics")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isInternalServerError())
        .andExpect(MockMvcResultMatchers.jsonPath("$.error").exists());
  }

  @Test
  public void testDescribeTopic() throws Exception {
    KafkaTopicResponse topicInfo =
        KafkaTopicResponse.builder()
            .name("MetadataChangeProposal_v1")
            .partitionCount(3)
            .replicationFactor(1)
            .build();
    when(kafkaAdminService.describeTopic("MetadataChangeProposal_v1")).thenReturn(topicInfo);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/kafka/topics/MetadataChangeProposal_v1")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$.name").value("MetadataChangeProposal_v1"))
        .andExpect(MockMvcResultMatchers.jsonPath("$.partitionCount").value(3));
  }

  @Test
  public void testDescribeTopic_NotFound() throws Exception {
    when(kafkaAdminService.describeTopic("unknown-topic"))
        .thenThrow(new KafkaAdminException.TopicNotFoundException("unknown-topic"));

    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/kafka/topics/unknown-topic")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound())
        .andExpect(MockMvcResultMatchers.jsonPath("$.error").exists());
  }

  @Test
  public void testCreateTopic() throws Exception {
    KafkaTopicResponse topicInfo =
        KafkaTopicResponse.builder()
            .name("MetadataChangeProposal_v1")
            .partitionCount(3)
            .replicationFactor(1)
            .build();
    when(kafkaAdminService.createDataHubTopic(any(), any(), any(), any())).thenReturn(topicInfo);

    String requestBody = "{\"numPartitions\": 3, \"replicationFactor\": 1}";
    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/kafka/topics/mcp")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$.name").value("MetadataChangeProposal_v1"));
  }

  @Test
  public void testCreateTopic_InvalidAlias() throws Exception {
    when(kafkaAdminService.createDataHubTopic(any(), any(), any(), any()))
        .thenThrow(
            new KafkaAdminException.InvalidAliasException(
                "invalid", java.util.Set.of("mcp", "mcl-versioned")));

    String requestBody = "{\"numPartitions\": 3}";
    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/kafka/topics/invalid")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(MockMvcResultMatchers.jsonPath("$.error").exists());
  }

  @Test
  public void testCreateTopic_AlreadyExists() throws Exception {
    when(kafkaAdminService.createDataHubTopic(any(), any(), any(), any()))
        .thenThrow(
            new KafkaAdminException.TopicAlreadyExistsException("MetadataChangeProposal_v1"));

    String requestBody = "{\"numPartitions\": 3}";
    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/kafka/topics/mcp")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(MockMvcResultMatchers.jsonPath("$.error").exists());
  }

  @Test
  public void testDeleteTopic() throws Exception {
    // No exception means success
    mockMvc
        .perform(
            MockMvcRequestBuilders.delete("/openapi/operations/kafka/topics/mcp")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());
  }

  @Test
  public void testDeleteTopic_InvalidAlias() throws Exception {
    org.mockito.Mockito.doThrow(
            new KafkaAdminException.InvalidAliasException(
                "invalid", java.util.Set.of("mcp", "mcl-versioned")))
        .when(kafkaAdminService)
        .deleteDataHubTopic("invalid");

    mockMvc
        .perform(
            MockMvcRequestBuilders.delete("/openapi/operations/kafka/topics/invalid")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(MockMvcResultMatchers.jsonPath("$.error").exists());
  }

  @Test
  public void testDeleteTopic_NotFound() throws Exception {
    org.mockito.Mockito.doThrow(new KafkaAdminException.TopicNotFoundException("mcp"))
        .when(kafkaAdminService)
        .deleteDataHubTopic("mcp");

    mockMvc
        .perform(
            MockMvcRequestBuilders.delete("/openapi/operations/kafka/topics/mcp")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(MockMvcResultMatchers.jsonPath("$.error").exists());
  }

  // ============================================================================
  // Message Retrieval Tests
  // ============================================================================

  @Test
  public void testGetMessages() throws Exception {
    io.datahubproject.openapi.operations.kafka.models.KafkaMessageResponse response =
        io.datahubproject.openapi.operations.kafka.models.KafkaMessageResponse.builder()
            .topic("MetadataChangeProposal_v1")
            .dataHubAlias("mcp")
            .messages(java.util.List.of())
            .count(0)
            .hasMore(false)
            .build();
    when(kafkaAdminService.getMessages(
            anyString(), anyInt(), anyLong(), anyInt(), anyString(), anyBoolean()))
        .thenReturn(response);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/kafka/messages/mcp")
                .param("partition", "0")
                .param("offset", "0")
                .param("count", "10")
                .param("format", "json")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$.topic").value("MetadataChangeProposal_v1"));
  }

  @Test
  public void testGetMessages_MissingPartition() throws Exception {
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/kafka/messages/mcp")
                .param("offset", "0")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest());
  }

  @Test
  public void testGetMessages_InvalidFormat() throws Exception {
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/kafka/messages/mcp")
                .param("partition", "0")
                .param("offset", "0")
                .param("format", "invalid-format")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.error")
                .value("Invalid format. Allowed values: json, avro, raw"));
  }

  @Test
  public void testGetMessages_InvalidAlias() throws Exception {
    when(kafkaAdminService.getMessages(
            anyString(), anyInt(), anyLong(), anyInt(), anyString(), anyBoolean()))
        .thenThrow(
            new KafkaAdminException.InvalidAliasException(
                "invalid", java.util.Set.of("mcp", "mcl-versioned")));

    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/kafka/messages/invalid")
                .param("partition", "0")
                .param("offset", "0")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(MockMvcResultMatchers.jsonPath("$.error").exists());
  }

  // ============================================================================
  // Consumer Group Reset Offsets Tests
  // ============================================================================

  @Test
  public void testResetConsumerGroupOffsets() throws Exception {
    java.util.List<
            io.datahubproject.openapi.operations.kafka.models.KafkaConsumerGroupResponse
                .PartitionResetInfo>
        resetInfos =
            java.util.List.of(
                io.datahubproject.openapi.operations.kafka.models.KafkaConsumerGroupResponse
                    .PartitionResetInfo.builder()
                    .partition(0)
                    .previousOffset(100L)
                    .newOffset(0L)
                    .build());
    when(kafkaAdminService.resetConsumerGroupOffsets(
            any(), any(), any(), any(), any(), anyBoolean()))
        .thenReturn(resetInfos);

    String requestBody = "{\"topic\": \"mcp\", \"strategy\": \"earliest\", \"dryRun\": true}";
    mockMvc
        .perform(
            MockMvcRequestBuilders.post(
                    "/openapi/operations/kafka/consumer-groups/datahub-mcp-consumer/offsets/reset")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$.dryRun").value(true))
        .andExpect(MockMvcResultMatchers.jsonPath("$.groupId").value("datahub-mcp-consumer"));
  }

  @Test
  public void testResetConsumerGroupOffsets_MissingTopic() throws Exception {
    String requestBody = "{\"strategy\": \"earliest\"}";
    mockMvc
        .perform(
            MockMvcRequestBuilders.post(
                    "/openapi/operations/kafka/consumer-groups/datahub-mcp-consumer/offsets/reset")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.error").value("Topic and strategy are required"));
  }

  @Test
  public void testResetConsumerGroupOffsets_MissingStrategy() throws Exception {
    String requestBody = "{\"topic\": \"mcp\"}";
    mockMvc
        .perform(
            MockMvcRequestBuilders.post(
                    "/openapi/operations/kafka/consumer-groups/datahub-mcp-consumer/offsets/reset")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.error").value("Topic and strategy are required"));
  }

  @Test
  public void testResetConsumerGroupOffsets_Error() throws Exception {
    when(kafkaAdminService.resetConsumerGroupOffsets(
            any(), any(), any(), any(), any(), anyBoolean()))
        .thenThrow(new KafkaAdminException("Consumer group is active"));

    String requestBody = "{\"topic\": \"mcp\", \"strategy\": \"earliest\"}";
    mockMvc
        .perform(
            MockMvcRequestBuilders.post(
                    "/openapi/operations/kafka/consumer-groups/datahub-mcp-consumer/offsets/reset")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(MockMvcResultMatchers.jsonPath("$.error").exists());
  }

  @SpringBootConfiguration
  @Import({KafkaControllerTestConfig.class, TracingInterceptor.class})
  @ComponentScan(basePackages = {"io.datahubproject.openapi.operations.kafka"})
  static class TestConfig {}

  @TestConfiguration
  public static class KafkaControllerTestConfig {
    @MockBean public MCPTraceReader mcpTraceReader;

    @MockBean
    @Qualifier("mclVersionedTraceReader")
    public MCLTraceReader mclTraceReader;

    @MockBean
    @Qualifier("mclTimeseriesTraceReader")
    public MCLTraceReader mclTimeseriesTraceReader;

    @MockBean
    @Qualifier("kafkaAdminService")
    public KafkaAdminService kafkaAdminService;

    @Bean
    public ObjectMapper objectMapper() {
      return new ObjectMapper();
    }

    @Bean(name = "systemOperationContext")
    public OperationContext systemOperationContext(ObjectMapper objectMapper) {
      SystemTelemetryContext systemTelemetryContext = mock(SystemTelemetryContext.class);
      return TestOperationContexts.systemContextTraceNoSearchAuthorization(
          () -> ObjectMapperContext.builder().objectMapper(objectMapper).build(),
          () -> systemTelemetryContext);
    }

    @Bean
    public EntityRegistry entityRegistry(
        @Qualifier("systemOperationContext") OperationContext systemOperationContext) {
      return systemOperationContext.getEntityRegistry();
    }

    @Bean
    @Primary
    public SystemTelemetryContext traceContext(
        @Qualifier("systemOperationContext") OperationContext systemOperationContext) {
      return systemOperationContext.getSystemTelemetryContext();
    }

    @Bean
    @Primary
    public AuthorizerChain authorizerChain() {
      AuthorizerChain authorizerChain = mock(AuthorizerChain.class);

      Authentication authentication = mock(Authentication.class);
      when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "datahub"));
      when(authorizerChain.authorize(any()))
          .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
      AuthenticationContext.setAuthentication(authentication);

      return authorizerChain;
    }
  }
}
