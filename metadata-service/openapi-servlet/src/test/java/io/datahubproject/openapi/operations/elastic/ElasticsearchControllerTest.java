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
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.request.AutocompleteRequestHandler;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.timeseries.TimeseriesIndexSizeResult;
import io.datahubproject.metadata.context.ObjectMapperContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.openapi.config.TracingInterceptor;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.opensearch.action.explain.ExplainResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.client.tasks.GetTaskResponse;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
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

  @Autowired private ElasticsearchController elasticsearchController;

  @Autowired private MockMvc mockMvc;

  @Autowired private SystemMetadataService mockSystemMetadataService;

  @Autowired private TimeseriesAspectService mockTimeseriesAspectService;

  @Autowired private EntitySearchService mockSearchService;

  @Autowired private EntityService<?> mockEntityService;

  @Autowired private AuthorizerChain authorizerChain;

  @Autowired private ESSearchDAO mockESSearchDAO;

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

  @Test
  public void testGetEntityRawWithEmptySet() throws Exception {
    // Prepare empty request body
    Set<String> urnStrs = new HashSet<>();

    ObjectMapper objectMapper = new ObjectMapper();
    String requestBody = objectMapper.writeValueAsString(urnStrs);

    // Mock empty response
    Map<Urn, Map<String, Object>> emptyMap = new HashMap<>();
    when(mockSearchService.raw(any(OperationContext.class), anySet())).thenReturn(emptyMap);

    // Test the endpoint
    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/elasticSearch/entity/raw")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$").isEmpty());
  }

  @Test
  public void testExplainSearchQueryWithAllParameters() throws Exception {
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

    // Test the endpoint with all parameters
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/elasticSearch/explainSearchQuery")
                .param("query", "complex test query")
                .param("documentId", TEST_URN_1.toString())
                .param("entityName", "dataset")
                .param("scrollId", "scroll123")
                .param("keepAlive", "5m")
                .param("size", "10")
                .param("filters", "{}")
                .param("searchFlags", "{\"fulltext\":true,\"skipHighlighting\":false}")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());
  }

  @Test
  public void testExplainSearchQueryDiff() throws Exception {
    // Mock explain responses for both documents
    ExplainResponse explainResponseA = mock(ExplainResponse.class);
    ExplainResponse explainResponseB = mock(ExplainResponse.class);

    when(mockSearchService.explain(
            any(OperationContext.class),
            eq("test query"),
            eq(URLEncoder.encode(TEST_URN_1.toString(), StandardCharsets.UTF_8)),
            eq("dataset"),
            any(),
            any(),
            any(),
            any(),
            anyInt()))
        .thenReturn(explainResponseA);

    when(mockSearchService.explain(
            any(OperationContext.class),
            eq("test query"),
            eq(URLEncoder.encode(TEST_URN_2.toString(), StandardCharsets.UTF_8)),
            eq("dataset"),
            any(),
            any(),
            any(),
            any(),
            anyInt()))
        .thenReturn(explainResponseB);

    // Test the endpoint
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/elasticSearch/explainSearchQueryDiff")
                .param("query", "test query")
                .param("documentIdA", TEST_URN_1.toString())
                .param("documentIdB", TEST_URN_2.toString())
                .param("entityName", "dataset")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.TEXT_PLAIN))
        .andExpect(status().isOk());
  }

  @Test
  public void testGetQueryScrollRequest() throws Exception {
    // Mock the search request components
    SearchRequest searchRequest = mock(SearchRequest.class);
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(QueryBuilders.matchAllQuery());
    searchSourceBuilder.size(10);
    when(searchRequest.source()).thenReturn(searchSourceBuilder);

    List<EntitySpec> entitySpecs = new ArrayList<>();
    Triple<SearchRequest, Filter, List<EntitySpec>> searchRequestComponents =
        Triple.of(searchRequest, null, entitySpecs);

    when(mockESSearchDAO.buildScrollRequest(
            any(OperationContext.class),
            any(),
            anyString(),
            anyString(),
            anyList(),
            anyInt(),
            any(),
            anyString(),
            any(),
            anyList()))
        .thenReturn(searchRequestComponents);

    // Test the endpoint
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/elasticSearch/query/scroll/request")
                .param("query", "test query")
                .param("entityName", "dataset")
                .param("size", "10")
                .param("scrollId", "scroll123")
                .param("keepAlive", "5m")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$.query").exists())
        .andExpect(MockMvcResultMatchers.jsonPath("$.size").value(10));
  }

  @Test
  public void testGetQuerySearchRequest() throws Exception {
    // Mock the search request components
    SearchRequest searchRequest = mock(SearchRequest.class);
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(QueryBuilders.matchQuery("name", "test"));
    searchSourceBuilder.size(20);
    when(searchRequest.source()).thenReturn(searchSourceBuilder);

    Filter filter = mock(Filter.class);
    List<EntitySpec> entitySpecs = new ArrayList<>();
    Triple<SearchRequest, Filter, List<EntitySpec>> searchRequestComponents =
        Triple.of(searchRequest, filter, entitySpecs);

    when(mockESSearchDAO.buildSearchRequest(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(),
            any(),
            anyInt(),
            anyInt(),
            anyList()))
        .thenReturn(searchRequestComponents);

    // Test the endpoint
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/elasticSearch/query/search/request")
                .param("query", "test")
                .param("entityName", "dataset")
                .param("size", "20")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$.query").exists())
        .andExpect(MockMvcResultMatchers.jsonPath("$.size").value(20));
  }

  @Test
  public void testGetQueryAutocompleteRequest() throws Exception {
    // Mock the autocomplete request components
    SearchRequest searchRequest = mock(SearchRequest.class);
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.size(10);
    when(searchRequest.source()).thenReturn(searchSourceBuilder);

    AutocompleteRequestHandler handler = mock(AutocompleteRequestHandler.class);
    Pair<SearchRequest, AutocompleteRequestHandler> searchRequestComponents =
        Pair.of(searchRequest, handler);

    when(mockESSearchDAO.buildAutocompleteRequest(
            any(OperationContext.class), anyString(), anyString(), anyString(), any(), anyInt()))
        .thenReturn(searchRequestComponents);

    // Test the endpoint
    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/operations/elasticSearch/query/autocomplete/request/name")
                .param("query", "test")
                .param("entityName", "dataset")
                .param("size", "10")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$.size").value(10));
  }

  @Test
  public void testGetQueryAggregateRequest() throws Exception {
    // Mock the aggregate request
    SearchRequest searchRequest = mock(SearchRequest.class);
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.size(0); // Aggregations typically have size 0
    searchSourceBuilder.aggregation(
        AggregationBuilders.terms("fieldName").field("fieldName").size(10));
    when(searchRequest.source()).thenReturn(searchSourceBuilder);

    when(mockESSearchDAO.buildAggregateByValue(
            any(OperationContext.class), anyList(), anyString(), any(), anyInt()))
        .thenReturn(searchRequest);

    // Test the endpoint
    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/operations/elasticSearch/query/aggregate/request/platform")
                .param("entityName", "dataset")
                .param("size", "10")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$.size").value(0))
        .andExpect(MockMvcResultMatchers.jsonPath("$.aggregations").exists());
  }

  @Test
  public void testRestoreIndicesGetWithAllParameters() throws Exception {
    // Mock restore indices response
    List<RestoreIndicesResult> restoreResults = new ArrayList<>();
    RestoreIndicesResult restoreResult = mock(RestoreIndicesResult.class);
    when(restoreResult.getIgnored()).thenReturn(5);
    when(restoreResult.getRowsMigrated()).thenReturn(100);
    when(restoreResult.getTimeSqlQueryMs()).thenReturn(1500L);
    when(restoreResult.getTimeGetRowMs()).thenReturn(200L);
    when(restoreResult.getTimeUrnMs()).thenReturn(300L);
    when(restoreResult.getTimeEntityRegistryCheckMs()).thenReturn(50L);
    when(restoreResult.getAspectCheckMs()).thenReturn(100L);
    when(restoreResult.getCreateRecordMs()).thenReturn(150L);
    when(restoreResult.getSendMessageMs()).thenReturn(75L);
    when(restoreResult.getDefaultAspectsCreated()).thenReturn(10L);
    when(restoreResult.getLastUrn()).thenReturn(TEST_URN_2.toString());
    when(restoreResult.getLastAspect()).thenReturn("datasetProperties");
    restoreResults.add(restoreResult);

    when(mockEntityService.restoreIndices(any(OperationContext.class), any(), any()))
        .thenReturn(restoreResults);

    // Test GET endpoint with all parameters
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/elasticSearch/restoreIndices")
                .param("aspectName", "datasetProperties")
                .param("urn", TEST_URN_1.toString())
                .param("urnLike", "%dataset%")
                .param("batchSize", "1000")
                .param("start", "100")
                .param("limit", "500")
                .param("gePitEpochMs", "1640995200000")
                .param("lePitEpochMs", "1672531200000")
                .param("createDefaultAspects", "true")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].ignored").value(5))
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].rowsMigrated").value(100))
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].timeSqlQueryMs").value(1500))
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].defaultAspectsCreated").value(10))
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].lastUrn").value(TEST_URN_2.toString()))
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].lastAspect").value("datasetProperties"));
  }

  @Test
  public void testRestoreIndicesPostWithMultipleAspects() throws Exception {
    // Mock restore indices response
    List<RestoreIndicesResult> restoreResults = new ArrayList<>();
    RestoreIndicesResult restoreResult1 = mock(RestoreIndicesResult.class);
    when(restoreResult1.getIgnored()).thenReturn(0);
    when(restoreResult1.getRowsMigrated()).thenReturn(50);
    when(restoreResult1.getTimeSqlQueryMs()).thenReturn(800L);
    when(restoreResult1.getLastAspect()).thenReturn("datasetProperties");

    RestoreIndicesResult restoreResult2 = mock(RestoreIndicesResult.class);
    when(restoreResult2.getIgnored()).thenReturn(2);
    when(restoreResult2.getRowsMigrated()).thenReturn(30);
    when(restoreResult2.getTimeSqlQueryMs()).thenReturn(600L);
    when(restoreResult2.getLastAspect()).thenReturn("schemaMetadata");

    restoreResults.add(restoreResult1);
    restoreResults.add(restoreResult2);

    when(mockEntityService.restoreIndices(
            any(OperationContext.class), any(), any(), anyInt(), anyBoolean()))
        .thenReturn(restoreResults);

    // Prepare request body
    Set<String> urnStrs = new HashSet<>();
    urnStrs.add(TEST_URN_1.toString());
    urnStrs.add(TEST_URN_2.toString());

    ObjectMapper objectMapper = new ObjectMapper();
    String requestBody = objectMapper.writeValueAsString(urnStrs);

    // Test POST endpoint with multiple aspects
    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/elasticSearch/restoreIndices")
                .content(requestBody)
                .param("aspectNames", "datasetProperties", "schemaMetadata")
                .param("batchSize", "200")
                .param("createDefaultAspects", "true")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].rowsMigrated").value(50))
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].lastAspect").value("datasetProperties"))
        .andExpect(MockMvcResultMatchers.jsonPath("$[1].rowsMigrated").value(30))
        .andExpect(MockMvcResultMatchers.jsonPath("$[1].lastAspect").value("schemaMetadata"));
  }

  @Test
  public void testSearchFlagsDeserialization() throws Exception {
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

    // Test with complex search flags
    String complexSearchFlags =
        "{\"fulltext\":true,\"skipHighlighting\":true,\"skipCache\":false,\"skipAggregates\":true}";

    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/operations/elasticSearch/explainSearchQuery")
                .param("query", "test query")
                .param("documentId", TEST_URN_1.toString())
                .param("entityName", "dataset")
                .param("searchFlags", complexSearchFlags)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());
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
    @MockBean public GraphService graphService;
    @MockBean public ESSearchDAO mockESSearchDAO;

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
            return SystemTelemetryContext.builder().tracer(tracer).build();
          });
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

    @Bean
    @Primary
    public GraphService graphService() {
      return graphService;
    }
  }
}
