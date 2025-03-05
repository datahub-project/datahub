package io.datahubproject.openapi.operations.kafka;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyCollection;
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
import io.datahubproject.metadata.context.TraceContext;
import io.datahubproject.openapi.config.TracingInterceptor;
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
        .andExpect(MockMvcResultMatchers.jsonPath("$['" + MCP_CONSUMER_GROUP + "']").exists())
        // Verify topic
        .andExpect(
            MockMvcResultMatchers.jsonPath("$['" + MCP_CONSUMER_GROUP + "']['" + TOPIC_1 + "']")
                .exists())
        // Verify partitions
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['" + MCP_CONSUMER_GROUP + "']['" + TOPIC_1 + "'].partitions['0'].offset")
                .value(1000))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['" + MCP_CONSUMER_GROUP + "']['" + TOPIC_1 + "'].partitions['0'].metadata")
                .value("metadata-p0"))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['" + MCP_CONSUMER_GROUP + "']['" + TOPIC_1 + "'].partitions['0'].lag")
                .value(100))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['" + MCP_CONSUMER_GROUP + "']['" + TOPIC_1 + "'].partitions['1'].offset")
                .value(2000))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['" + MCP_CONSUMER_GROUP + "']['" + TOPIC_1 + "'].partitions['1'].lag")
                .value(500))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['" + MCP_CONSUMER_GROUP + "']['" + TOPIC_1 + "'].partitions['2'].offset")
                .value(3000))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['" + MCP_CONSUMER_GROUP + "']['" + TOPIC_1 + "'].partitions['2'].lag")
                .value(50))
        // Verify metrics
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['" + MCP_CONSUMER_GROUP + "']['" + TOPIC_1 + "'].metrics.maxLag")
                .value(500))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['" + MCP_CONSUMER_GROUP + "']['" + TOPIC_1 + "'].metrics.medianLag")
                .value(100))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['" + MCP_CONSUMER_GROUP + "']['" + TOPIC_1 + "'].metrics.totalLag")
                .value(650))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['" + MCP_CONSUMER_GROUP + "']['" + TOPIC_1 + "'].metrics.avgLag")
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
        .andExpect(MockMvcResultMatchers.jsonPath("$['" + MCP_CONSUMER_GROUP + "']").exists())
        .andExpect(
            MockMvcResultMatchers.jsonPath("$['" + MCP_CONSUMER_GROUP + "']['" + TOPIC_1 + "']")
                .exists())
        // Partitions should not be included
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['" + MCP_CONSUMER_GROUP + "']['" + TOPIC_1 + "'].partitions")
                .doesNotExist())
        // Metrics should still be included
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['" + MCP_CONSUMER_GROUP + "']['" + TOPIC_1 + "'].metrics.maxLag")
                .value(500))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['" + MCP_CONSUMER_GROUP + "']['" + TOPIC_1 + "'].metrics.totalLag")
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
        .andExpect(MockMvcResultMatchers.jsonPath("$['" + MCL_CONSUMER_GROUP + "']").exists())
        // Verify topic
        .andExpect(
            MockMvcResultMatchers.jsonPath("$['" + MCL_CONSUMER_GROUP + "']['" + TOPIC_2 + "']")
                .exists())
        // Verify partitions
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['" + MCL_CONSUMER_GROUP + "']['" + TOPIC_2 + "'].partitions['0'].offset")
                .value(1000))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['" + MCL_CONSUMER_GROUP + "']['" + TOPIC_2 + "'].partitions['1'].offset")
                .value(2000))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['" + MCL_CONSUMER_GROUP + "']['" + TOPIC_2 + "'].partitions['2'].offset")
                .value(3000))
        // Verify metrics
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['" + MCL_CONSUMER_GROUP + "']['" + TOPIC_2 + "'].metrics.maxLag")
                .value(500))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['" + MCL_CONSUMER_GROUP + "']['" + TOPIC_2 + "'].metrics.totalLag")
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
            MockMvcResultMatchers.jsonPath("$['" + MCL_TIMESERIES_CONSUMER_GROUP + "']").exists())
        // Verify topic
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['" + MCL_TIMESERIES_CONSUMER_GROUP + "']['" + TOPIC_3 + "']")
                .exists())
        // Verify partitions
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['"
                        + MCL_TIMESERIES_CONSUMER_GROUP
                        + "']['"
                        + TOPIC_3
                        + "'].partitions['0'].offset")
                .value(1000))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['"
                        + MCL_TIMESERIES_CONSUMER_GROUP
                        + "']['"
                        + TOPIC_3
                        + "'].partitions['1'].offset")
                .value(2000))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['"
                        + MCL_TIMESERIES_CONSUMER_GROUP
                        + "']['"
                        + TOPIC_3
                        + "'].partitions['2'].offset")
                .value(3000))
        // Verify metrics
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['" + MCL_TIMESERIES_CONSUMER_GROUP + "']['" + TOPIC_3 + "'].metrics.maxLag")
                .value(500))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['"
                        + MCL_TIMESERIES_CONSUMER_GROUP
                        + "']['"
                        + TOPIC_3
                        + "'].metrics.totalLag")
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
        // Should return an empty response
        .andExpect(MockMvcResultMatchers.jsonPath("$").isEmpty());
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

    @Bean
    public ObjectMapper objectMapper() {
      return new ObjectMapper();
    }

    @Bean(name = "systemOperationContext")
    public OperationContext systemOperationContext(ObjectMapper objectMapper) {
      TraceContext traceContext = mock(TraceContext.class);
      return TestOperationContexts.systemContextTraceNoSearchAuthorization(
          () -> ObjectMapperContext.builder().objectMapper(objectMapper).build(),
          () -> traceContext);
    }

    @Bean
    public EntityRegistry entityRegistry(
        @Qualifier("systemOperationContext") OperationContext systemOperationContext) {
      return systemOperationContext.getEntityRegistry();
    }

    @Bean
    @Primary
    public TraceContext traceContext(
        @Qualifier("systemOperationContext") OperationContext systemOperationContext) {
      return systemOperationContext.getTraceContext();
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
