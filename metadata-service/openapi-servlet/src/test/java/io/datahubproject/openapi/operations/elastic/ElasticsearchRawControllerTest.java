package io.datahubproject.openapi.operations.elastic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.testng.Assert.assertNotNull;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.config.GlobalControllerExceptionHandler;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@WebMvcTest(ElasticsearchRawController.class)
@ContextConfiguration(classes = ElasticsearchControllerTest.ElasticsearchControllerTestConfig.class)
@Import({AuthUtil.class, GlobalControllerExceptionHandler.class})
@AutoConfigureMockMvc
public class ElasticsearchRawControllerTest extends AbstractTestNGSpringContextTests {

  private static final Urn TEST_URN_1 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)");
  private static final Urn TEST_URN_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)");
  private static final Urn TEST_URN_3 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,PROD)");

  @Autowired private ElasticsearchRawController elasticsearchRawController;

  @Autowired private MockMvc mockMvc;

  @Autowired private SystemMetadataService mockSystemMetadataService;

  @Autowired private TimeseriesAspectService mockTimeseriesAspectService;

  @Autowired private EntitySearchService mockSearchService;

  @Autowired private GraphService mockGraphService;

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
    assertNotNull(elasticsearchRawController);
  }

  @Test
  public void testGetEntityRaw() throws Exception {
    // Mock raw entity response
    Map<Urn, Map<String, Object>> rawEntityMap = new HashMap<>();

    Map<String, Object> entity1Map = new HashMap<>();
    entity1Map.put("urn", TEST_URN_1.toString());
    entity1Map.put("name", "Sample Table");
    entity1Map.put("platform", "hive");
    entity1Map.put("_index", "datahub_entity_v2");
    entity1Map.put("_id", TEST_URN_1.toString());

    Map<String, Object> entity2Map = new HashMap<>();
    entity2Map.put("urn", TEST_URN_2.toString());
    entity2Map.put("name", "Test Table");
    entity2Map.put("platform", "snowflake");
    entity2Map.put("_index", "datahub_entity_v2");
    entity2Map.put("_id", TEST_URN_2.toString());

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
                .value("Sample Table"))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$['" + TEST_URN_1.toString() + "'].platform")
                .value("hive"))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$['" + TEST_URN_2.toString() + "'].name")
                .value("Test Table"))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$['" + TEST_URN_2.toString() + "'].platform")
                .value("snowflake"));
  }

  @Test
  public void testGetEntityRawUnauthorized() throws Exception {
    // Setup AuthorizerChain to deny access
    when(authorizerChain.authorize(any()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.DENY, ""));

    // Prepare request body
    Set<String> urnStrs = new HashSet<>();
    urnStrs.add(TEST_URN_1.toString());

    ObjectMapper objectMapper = new ObjectMapper();
    String requestBody = objectMapper.writeValueAsString(urnStrs);

    // Test the endpoint - should return 403
    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/elasticSearch/entity/raw")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isForbidden());
  }

  @Test
  public void testGetSystemMetadataRaw() throws Exception {
    // Mock raw system metadata response
    Map<Urn, Map<String, Map<String, Object>>> rawSystemMetadataMap = new HashMap<>();

    Map<String, Map<String, Object>> urn1Aspects = new HashMap<>();
    Map<String, Object> statusAspect = new HashMap<>();
    statusAspect.put("removed", false);
    statusAspect.put("_index", "system_metadata_service_v1");
    statusAspect.put("_id", TEST_URN_1.toString() + "_status");
    urn1Aspects.put("status", statusAspect);

    Map<String, Object> propertiesAspect = new HashMap<>();
    propertiesAspect.put("description", "Sample dataset");
    propertiesAspect.put("_index", "system_metadata_service_v1");
    propertiesAspect.put("_id", TEST_URN_1.toString() + "_datasetProperties");
    urn1Aspects.put("datasetProperties", propertiesAspect);

    rawSystemMetadataMap.put(TEST_URN_1, urn1Aspects);

    when(mockSystemMetadataService.raw(any(OperationContext.class), anyMap()))
        .thenReturn(rawSystemMetadataMap);

    // Prepare request body
    Map<String, Set<String>> urnAspects = new HashMap<>();
    Set<String> aspects = new HashSet<>();
    aspects.add("status");
    aspects.add("datasetProperties");
    urnAspects.put(TEST_URN_1.toString(), aspects);

    ObjectMapper objectMapper = new ObjectMapper();
    String requestBody = objectMapper.writeValueAsString(urnAspects);

    // Test the endpoint
    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/elasticSearch/systemmetadata/raw")
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
            MockMvcResultMatchers.jsonPath("$['" + TEST_URN_1.toString() + "'].status.removed")
                .value(false))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['" + TEST_URN_1.toString() + "'].datasetProperties.description")
                .value("Sample dataset"));
  }

  @Test
  public void testGetTimeseriesRaw() throws Exception {
    // Mock raw timeseries response
    Map<Urn, Map<String, Map<String, Object>>> rawTimeseriesMap = new HashMap<>();

    Map<String, Map<String, Object>> urn1Aspects = new HashMap<>();
    Map<String, Object> profileAspect = new HashMap<>();
    profileAspect.put("timestampMillis", 1234567890L);
    profileAspect.put("rowCount", 1000000L);
    profileAspect.put("columnCount", 25);
    profileAspect.put("_index", "dataset_datasetprofileaspect_v1");
    profileAspect.put("_id", TEST_URN_1.toString() + "_datasetProfile_1234567890");
    urn1Aspects.put("datasetProfile", profileAspect);

    rawTimeseriesMap.put(TEST_URN_1, urn1Aspects);

    when(mockTimeseriesAspectService.raw(any(OperationContext.class), anyMap()))
        .thenReturn(rawTimeseriesMap);

    // Prepare request body
    Map<String, Set<String>> urnAspects = new HashMap<>();
    Set<String> aspects = new HashSet<>();
    aspects.add("datasetProfile");
    urnAspects.put(TEST_URN_1.toString(), aspects);

    ObjectMapper objectMapper = new ObjectMapper();
    String requestBody = objectMapper.writeValueAsString(urnAspects);

    // Test the endpoint
    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/elasticSearch/timeseries/raw")
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
            MockMvcResultMatchers.jsonPath(
                    "$['" + TEST_URN_1.toString() + "'].datasetProfile.timestampMillis")
                .value(1234567890L))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['" + TEST_URN_1.toString() + "'].datasetProfile.rowCount")
                .value(1000000L))
        .andExpect(
            MockMvcResultMatchers.jsonPath(
                    "$['" + TEST_URN_1.toString() + "'].datasetProfile.columnCount")
                .value(25));
  }

  @Test
  public void testGetGraphRaw() throws Exception {
    // Mock raw graph response
    List<Map<String, Object>> rawGraphList = new ArrayList<>();

    Map<String, Object> edge1 = new HashMap<>();
    edge1.put("source", TEST_URN_1.toString());
    edge1.put("destination", TEST_URN_2.toString());
    edge1.put("relationshipType", "DownstreamOf");
    edge1.put("_index", "graph_service_v1");
    edge1.put("_id", "edge1_id");

    Map<String, Object> edge2 = new HashMap<>();
    edge2.put("source", TEST_URN_2.toString());
    edge2.put("destination", TEST_URN_3.toString());
    edge2.put("relationshipType", "DownstreamOf");
    edge2.put("_index", "graph_service_v1");
    edge2.put("_id", "edge2_id");

    rawGraphList.add(edge1);
    rawGraphList.add(edge2);

    when(mockGraphService.raw(any(OperationContext.class), anyList())).thenReturn(rawGraphList);

    // Prepare request body
    List<GraphService.EdgeTuple> edgeTuples = new ArrayList<>();
    GraphService.EdgeTuple tuple1 = new GraphService.EdgeTuple();
    tuple1.setA(TEST_URN_1.toString());
    tuple1.setB(TEST_URN_2.toString());
    tuple1.setRelationshipType("DownstreamOf");
    edgeTuples.add(tuple1);

    GraphService.EdgeTuple tuple2 = new GraphService.EdgeTuple();
    tuple2.setA(TEST_URN_2.toString());
    tuple2.setB(TEST_URN_3.toString());
    tuple2.setRelationshipType("DownstreamOf");
    edgeTuples.add(tuple2);

    ObjectMapper objectMapper = new ObjectMapper();
    String requestBody = objectMapper.writeValueAsString(edgeTuples);

    // Test the endpoint
    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/elasticSearch/graph/raw")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andDo(
            result -> {
              String responseContent = result.getResponse().getContentAsString();
              System.out.println("Response content: " + responseContent);
            })
        .andExpect(MockMvcResultMatchers.jsonPath("$").isArray())
        .andExpect(MockMvcResultMatchers.jsonPath("$.length()").value(2))
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].source").value(TEST_URN_1.toString()))
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].destination").value(TEST_URN_2.toString()))
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].relationshipType").value("DownstreamOf"))
        .andExpect(MockMvcResultMatchers.jsonPath("$[1].source").value(TEST_URN_2.toString()))
        .andExpect(MockMvcResultMatchers.jsonPath("$[1].destination").value(TEST_URN_3.toString()));
  }

  @Test
  public void testInvalidUrnFormatWithProperExceptionHandling() throws Exception {
    Set<String> urnStrs = new HashSet<>();
    urnStrs.add("invalid:urn:format");

    ObjectMapper objectMapper = new ObjectMapper();
    String requestBody = objectMapper.writeValueAsString(urnStrs);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/operations/elasticSearch/entity/raw")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(MockMvcResultMatchers.jsonPath("$.error").exists());
  }
}
