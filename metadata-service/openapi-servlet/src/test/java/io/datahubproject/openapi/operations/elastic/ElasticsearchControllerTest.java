package io.datahubproject.openapi.operations.elastic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesResult;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.timeseries.TimeseriesIndexSizeResult;
import io.datahubproject.metadata.context.ObjectMapperContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.TraceContext;
import io.datahubproject.openapi.config.TracingInterceptor;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.opensearch.action.explain.ExplainResponse;
import org.opensearch.client.tasks.GetTaskResponse;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.TaskInfo;
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

@SpringBootTest(classes = ElasticsearchControllerTest.TestConfig.class)
@AutoConfigureWebMvc
@AutoConfigureMockMvc
public class ElasticsearchControllerTest extends AbstractTestNGSpringContextTests {

  private static final String TEST_NODE_ID = "aB1cdEf2GHI-JKLMnoPQr3";
  private static final String TEST_TASK_ID = "123456";
  private static final String TEST_TASK = TEST_NODE_ID + ":" + TEST_TASK_ID;

  private static final Urn TEST_URN_1 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,PROD)");
  private static final Urn TEST_URN_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)");

  private OperationContext opContext = TestOperationContexts.systemContextNoValidate();

  @Autowired private ElasticsearchController elasticsearchController;

  @Autowired private MockMvc mockMvc;

  @Autowired private SystemMetadataService mockSystemMetadataService;

  @Autowired private TimeseriesAspectService mockTimeseriesAspectService;

  @Autowired private EntitySearchService mockSearchService;

  @Autowired private EntityService<?> mockEntityService;

  @Autowired private AuthorizerChain authorizerChain;

  @BeforeMethod
  public void setupMocks() {
    // Setup Authentication
    Authentication authentication = mock(Authentication.class);
    when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "datahub"));
    AuthenticationContext.setAuthentication(authentication);

    // Setup AuthorizerChain to allow access
    when(authorizerChain.authorize(any()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
  }

  @Test
  public void initTest() {
    assertNotNull(elasticsearchController);
  }

  @Test
  public void testGetTaskStatus() throws Exception {
    // Mock task response
    GetTaskResponse taskResponse = mock(GetTaskResponse.class);
    TaskInfo taskInfo = mock(TaskInfo.class);
    TaskId taskId = mock(TaskId.class);
    when(taskId.toString()).thenReturn(TEST_TASK);
    when(taskInfo.getTaskId()).thenReturn(taskId);
    when(taskInfo.getRunningTimeNanos()).thenReturn(5000000L);
    when(taskResponse.getTaskInfo()).thenReturn(taskInfo);
    when(taskResponse.isCompleted()).thenReturn(false);

    when(mockSystemMetadataService.getTaskStatus(
            eq(TEST_NODE_ID), eq(Long.parseLong(TEST_TASK_ID))))
        .thenReturn(Optional.of(taskResponse));

    // Test the endpoint
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/elasticSearch/getTaskStatus")
                .param("task", TEST_TASK)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andDo(
            result -> {
              String responseContent = result.getResponse().getContentAsString();
              System.out.println("Response content: " + responseContent);
            })
        .andExpect(MockMvcResultMatchers.jsonPath("$.completed").value(false))
        .andExpect(MockMvcResultMatchers.jsonPath("$.taskId").value(TEST_TASK))
        .andExpect(MockMvcResultMatchers.jsonPath("$.runTimeNanos").value(5000000));
  }

  @Test
  public void testGetTaskStatusInvalidTaskId() throws Exception {
    // Test with invalid task ID format
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/elasticSearch/getTaskStatus")
                .param("task", "invalid-task-id")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest());
  }

  @Test
  public void testGetIndexSizes() throws Exception {
    // Mock index size response
    List<TimeseriesIndexSizeResult> indexSizeResults = new ArrayList<>();
    TimeseriesIndexSizeResult result1 = mock(TimeseriesIndexSizeResult.class);
    when(result1.getAspectName()).thenReturn("datasetProfile");
    when(result1.getEntityName()).thenReturn("dataset");
    when(result1.getIndexName()).thenReturn("datasetProfile_v1");
    when(result1.getSizeInMb()).thenReturn(120.5);

    TimeseriesIndexSizeResult result2 = mock(TimeseriesIndexSizeResult.class);
    when(result2.getAspectName()).thenReturn("usageStats");
    when(result2.getEntityName()).thenReturn("dataset");
    when(result2.getIndexName()).thenReturn("usageStats_v1");
    when(result2.getSizeInMb()).thenReturn(350.2);

    indexSizeResults.add(result1);
    indexSizeResults.add(result2);

    when(mockTimeseriesAspectService.getIndexSizes(any(OperationContext.class)))
        .thenReturn(indexSizeResults);

    // Test the endpoint
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/elasticSearch/getIndexSizes")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andDo(
            result -> {
              String responseContent = result.getResponse().getContentAsString();
              System.out.println("Response content: " + responseContent);
            })
        .andExpect(MockMvcResultMatchers.jsonPath("$.sizes").isArray())
        .andExpect(MockMvcResultMatchers.jsonPath("$.sizes.length()").value(2))
        .andExpect(MockMvcResultMatchers.jsonPath("$.sizes[0].aspectName").value("datasetProfile"))
        .andExpect(MockMvcResultMatchers.jsonPath("$.sizes[0].entityName").value("dataset"))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.sizes[0].indexName").value("datasetProfile_v1"))
        .andExpect(MockMvcResultMatchers.jsonPath("$.sizes[0].sizeMb").value(120.5))
        .andExpect(MockMvcResultMatchers.jsonPath("$.sizes[1].aspectName").value("usageStats"))
        .andExpect(MockMvcResultMatchers.jsonPath("$.sizes[1].sizeMb").value(350.2));
  }

  @Test
  public void testGetEntityRaw() throws Exception {
    // Mock raw entity response
    Map<Urn, Map<String, Object>> rawEntityMap = new HashMap<>();

    Map<String, Object> entity1Map = new HashMap<>();
    entity1Map.put("urn", TEST_URN_1.toString());
    entity1Map.put("name", "Test Dataset 1");
    entity1Map.put("platform", "hdfs");

    Map<String, Object> entity2Map = new HashMap<>();
    entity2Map.put("urn", TEST_URN_2.toString());
    entity2Map.put("name", "Test Dataset 2");
    entity2Map.put("platform", "snowflake");

    rawEntityMap.put(TEST_URN_1, entity1Map);
    rawEntityMap.put(TEST_URN_2, entity2Map);

    when(mockSearchService.raw(any(OperationContext.class), anySet())).thenReturn(rawEntityMap);

    // Prepare request body
    Set<String> urnStrs = new HashSet<>();
    urnStrs.add(TEST_URN_1.toString());
    urnStrs.add(TEST_URN_2.toString());

    ObjectMapper objectMapper = new ObjectMapper();
    String requestBody = objectMapper.writeValueAsString(urnStrs);

    // Test the endpoint
    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/elasticSearch/entity/raw")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andDo(
            result -> {
              String responseContent = result.getResponse().getContentAsString();
              System.out.println("Response content: " + responseContent);
            })
        .andExpect(
            MockMvcResultMatchers.jsonPath("$['" + TEST_URN_1.toString() + "'].name")
                .value("Test Dataset 1"))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$['" + TEST_URN_1.toString() + "'].platform")
                .value("hdfs"))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$['" + TEST_URN_2.toString() + "'].name")
                .value("Test Dataset 2"))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$['" + TEST_URN_2.toString() + "'].platform")
                .value("snowflake"));
  }

  @Test
  public void testExplainSearchQuery() throws Exception {
    // Mock explain response
    ExplainResponse explainResponse = mock(ExplainResponse.class);
    when(mockSearchService.explain(
            any(OperationContext.class),
            anyString(),
            anyString(),
            anyString(),
            any(),
            any(),
            anyString(),
            anyString(),
            anyInt(),
            anyList()))
        .thenReturn(explainResponse);

    // Test the endpoint
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/elasticSearch/explainSearchQuery")
                .param("query", "test query")
                .param("documentId", TEST_URN_1.toString())
                .param("entityName", "dataset")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());
  }

  @Test
  public void testRestoreIndices() throws Exception {
    // Mock restore indices response based on actual format
    List<RestoreIndicesResult> restoreResults = new ArrayList<>();
    RestoreIndicesResult restoreResult = mock(RestoreIndicesResult.class);
    when(restoreResult.getIgnored()).thenReturn(0);
    when(restoreResult.getRowsMigrated()).thenReturn(0);
    when(restoreResult.getTimeSqlQueryMs()).thenReturn(0l);
    when(restoreResult.getTimeGetRowMs()).thenReturn(0l);
    when(restoreResult.getTimeUrnMs()).thenReturn(0l);
    when(restoreResult.getTimeEntityRegistryCheckMs()).thenReturn(0l);
    when(restoreResult.getAspectCheckMs()).thenReturn(0l);
    when(restoreResult.getCreateRecordMs()).thenReturn(0l);
    when(restoreResult.getSendMessageMs()).thenReturn(0l);
    when(restoreResult.getDefaultAspectsCreated()).thenReturn(0l);
    when(restoreResult.getLastUrn()).thenReturn("");
    when(restoreResult.getLastAspect()).thenReturn("");
    restoreResults.add(restoreResult);

    when(mockEntityService.restoreIndices(
            any(OperationContext.class), any(), any(), anyInt(), anyBoolean()))
        .thenReturn(restoreResults);

    // Prepare request body
    Set<String> urnStrs = new HashSet<>();
    urnStrs.add(TEST_URN_1.toString());

    ObjectMapper objectMapper = new ObjectMapper();
    String requestBody = objectMapper.writeValueAsString(urnStrs);

    // Test POST endpoint
    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/elasticSearch/restoreIndices")
                .content(requestBody)
                .param("aspectNames", "datasetProperties")
                .param("batchSize", "100")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andDo(
            result -> {
              String responseContent = result.getResponse().getContentAsString();
              System.out.println("Response content: " + responseContent);
            })
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].ignored").exists())
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].rowsMigrated").exists())
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].timeSqlQueryMs").exists())
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].lastUrn").exists());
  }

  @Test
  public void testRestoreIndicesGet() throws Exception {
    // Mock restore indices response based on actual format
    List<RestoreIndicesResult> restoreResults = new ArrayList<>();
    RestoreIndicesResult restoreResult = mock(RestoreIndicesResult.class);
    when(restoreResult.getIgnored()).thenReturn(0);
    when(restoreResult.getRowsMigrated()).thenReturn(0);
    when(restoreResult.getTimeSqlQueryMs()).thenReturn(0L);
    when(restoreResult.getTimeGetRowMs()).thenReturn(0L);
    when(restoreResult.getTimeUrnMs()).thenReturn(0L);
    when(restoreResult.getTimeEntityRegistryCheckMs()).thenReturn(0L);
    when(restoreResult.getAspectCheckMs()).thenReturn(0L);
    when(restoreResult.getCreateRecordMs()).thenReturn(0L);
    when(restoreResult.getSendMessageMs()).thenReturn(0L);
    when(restoreResult.getDefaultAspectsCreated()).thenReturn(0L);
    when(restoreResult.getLastUrn()).thenReturn("");
    when(restoreResult.getLastAspect()).thenReturn("");
    restoreResults.add(restoreResult);

    when(mockEntityService.restoreIndices(any(OperationContext.class), any(), any()))
        .thenReturn(restoreResults);

    // Test GET endpoint
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/elasticSearch/restoreIndices")
                .param("aspectName", "datasetProperties")
                .param("urn", TEST_URN_1.toString())
                .param("batchSize", "500")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andDo(
            result -> {
              String responseContent = result.getResponse().getContentAsString();
              System.out.println("Response content: " + responseContent);
            })
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].ignored").exists())
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].rowsMigrated").exists())
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].lastUrn").exists())
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].lastAspect").exists());
  }

  @SpringBootConfiguration
  @Import({ElasticsearchControllerTestConfig.class, TracingInterceptor.class})
  @ComponentScan(basePackages = {"io.datahubproject.openapi.operations.elastic"})
  static class TestConfig {}

  @TestConfiguration
  public static class ElasticsearchControllerTestConfig {
    @MockBean public SystemMetadataService systemMetadataService;
    @MockBean public TimeseriesAspectService timeseriesAspectService;
    @MockBean public EntitySearchService searchService;
    @MockBean public EntityService<?> entityService;

    @Bean
    public ObjectMapper objectMapper() {
      return new ObjectMapper();
    }

    @Bean(name = "systemOperationContext")
    public OperationContext systemOperationContext(ObjectMapper objectMapper) {
      return TestOperationContexts.systemContextTraceNoSearchAuthorization(
          () -> ObjectMapperContext.builder().objectMapper(objectMapper).build(),
          () -> {
            // Set up OpenTelemetry SDK for testing
            SdkTracerProvider tracerProvider = SdkTracerProvider.builder().build();
            OpenTelemetry openTelemetry =
                OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();

            // Create a tracer
            Tracer tracer = openTelemetry.getTracer("test-tracer");
            return TraceContext.builder().tracer(tracer).build();
          });
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
