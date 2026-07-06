package io.datahubproject.openapi.v3.controller;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.openapi.config.GlobalControllerExceptionHandler;
import io.datahubproject.openapi.config.SpringWebConfig;
import io.datahubproject.openapi.config.TracingInterceptor;
import io.datahubproject.openapi.v3.models.ConjunctiveCriterion;
import io.datahubproject.openapi.v3.models.Criterion;
import io.datahubproject.openapi.v3.models.ScrollRelationshipsRequestBody;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureWebMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SpringBootTest(classes = {SpringWebConfig.class})
@ComponentScan(basePackages = {"io.datahubproject.openapi.v3.controller.RelationshipController"})
@Import({
  SpringWebConfig.class,
  TracingInterceptor.class,
  RelationshipController.class,
  LineageController.class,
  RelationshipControllerTest.RelationshipControllerTestConfig.class,
  GlobalControllerExceptionHandler.class,
})
@AutoConfigureWebMvc
@AutoConfigureMockMvc
public class RelationshipControllerTest extends AbstractTestNGSpringContextTests {
  @MockitoBean private ConfigurationProvider configurationProvider;
  @MockitoBean private EntityRegistry entityRegistry;
  @MockitoBean private SystemTelemetryContext systemTelemetryContext;

  @Autowired private RelationshipController relationshipController;
  @Autowired private MockMvc mockMvc;
  @Autowired private GraphService mockGraphService;
  @Autowired private ObjectMapper objectMapper;

  @BeforeMethod
  public void setup() {
    org.mockito.MockitoAnnotations.openMocks(this);
  }

  @TestConfiguration
  public static class RelationshipControllerTestConfig {

    @Bean
    public ObjectMapper objectMapper() {
      return new ObjectMapper();
    }

    @Bean(name = "systemOperationContext")
    public OperationContext systemOperationContext() {
      return TestOperationContexts.systemContextNoSearchAuthorization();
    }

    @Bean("graphService")
    @Primary
    public ElasticSearchGraphService graphService() {
      return mock(ElasticSearchGraphService.class);
    }

    @Bean
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

  @Test
  public void initTest() {
    assertNotNull(relationshipController);
  }

  @Test
  public void testGetRelationshipsByTypeWithSliceParameters() throws Exception {
    String relationshipType = "DownstreamOf";

    // Simple test data - empty result for testing slice parameter handling
    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, "test-scroll-id", Arrays.asList());

    // Use ArgumentCaptor to capture the OperationContext and verify slice options
    ArgumentCaptor<OperationContext> opContextCaptor =
        ArgumentCaptor.forClass(OperationContext.class);

    when(mockGraphService.scrollRelatedEntities(
            opContextCaptor.capture(),
            isNull(),
            any(),
            isNull(),
            any(),
            anySet(),
            any(),
            any(),
            isNull(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/v3/relationship/" + relationshipType)
                .param("sliceId", "0")
                .param("sliceMax", "2")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful())
        .andExpect(jsonPath("$.results").isArray())
        .andExpect(jsonPath("$.scrollId").value("test-scroll-id"));

    // Verify that slice options were properly set in the operation context
    OperationContext capturedOpContext = opContextCaptor.getValue();
    assertNotNull(capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions());
    assertNotNull(capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions().getId());
    assertNotNull(capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions().getMax());
    assertEquals(
        capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions().getId().intValue(),
        0);
    assertEquals(
        capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions().getMax().intValue(),
        2);
  }

  @Test
  public void testGetRelationshipsByTypeWithoutSliceParameters() throws Exception {
    String relationshipType = "DownstreamOf";

    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, null, Arrays.asList());

    // Use ArgumentCaptor to capture the OperationContext and verify no slice options
    ArgumentCaptor<OperationContext> opContextCaptor =
        ArgumentCaptor.forClass(OperationContext.class);

    when(mockGraphService.scrollRelatedEntities(
            opContextCaptor.capture(),
            isNull(),
            any(),
            isNull(),
            any(),
            anySet(),
            any(),
            any(),
            isNull(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/v3/relationship/" + relationshipType)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    // Verify that slice options were not set
    OperationContext capturedOpContext = opContextCaptor.getValue();
    assertNull(capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions());
  }

  @Test
  public void testGetRelationshipsByTypeWithOnlyOneSliceParameter() throws Exception {
    String relationshipType = "DownstreamOf";

    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, null, Arrays.asList());

    // Use ArgumentCaptor to capture the OperationContext and verify no slice options
    ArgumentCaptor<OperationContext> opContextCaptor =
        ArgumentCaptor.forClass(OperationContext.class);

    when(mockGraphService.scrollRelatedEntities(
            opContextCaptor.capture(),
            isNull(),
            any(),
            isNull(),
            any(),
            anySet(),
            any(),
            any(),
            isNull(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    // Test with only sliceId - should not set slice options (both parameters are required)
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/v3/relationship/" + relationshipType)
                .param("sliceId", "0")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    // Verify that slice options were not set (both parameters are required)
    OperationContext capturedOpContext = opContextCaptor.getValue();
    assertNull(capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions());
  }

  @Test
  public void testGetRelationshipsByEntityWithSliceParameters() throws Exception {
    String entityUrn = "urn:li:dataset:(urn:li:dataPlatform:testPlatform,test,PROD)";

    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(1, 10, "test-scroll-id", Arrays.asList());

    // Use ArgumentCaptor to capture the OperationContext and verify slice options
    ArgumentCaptor<OperationContext> opContextCaptor =
        ArgumentCaptor.forClass(OperationContext.class);

    when(mockGraphService.scrollRelatedEntities(
            opContextCaptor.capture(),
            isNull(),
            any(),
            isNull(),
            any(),
            anySet(),
            any(),
            any(),
            isNull(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/v3/relationship/{entityName}/{entityUrn}", "dataset", entityUrn)
                .param("sliceId", "1")
                .param("sliceMax", "3")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful())
        .andExpect(jsonPath("$.results").isArray())
        .andExpect(jsonPath("$.scrollId").value("test-scroll-id"));

    // Verify that slice options were properly set in the operation context
    OperationContext capturedOpContext = opContextCaptor.getValue();
    assertNotNull(capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions());
    assertNotNull(capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions().getId());
    assertNotNull(capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions().getMax());
    assertEquals(
        capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions().getId().intValue(),
        1);
    assertEquals(
        capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions().getMax().intValue(),
        3);
  }

  @Test
  public void testGetRelationshipsByEntityWithoutSliceParameters() throws Exception {
    String entityUrn = "urn:li:dataset:(urn:li:dataPlatform:testPlatform,test,PROD)";

    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, null, Arrays.asList());

    // Use ArgumentCaptor to capture the OperationContext and verify no slice options
    ArgumentCaptor<OperationContext> opContextCaptor =
        ArgumentCaptor.forClass(OperationContext.class);

    when(mockGraphService.scrollRelatedEntities(
            opContextCaptor.capture(),
            isNull(),
            any(),
            isNull(),
            any(),
            anySet(),
            any(),
            any(),
            isNull(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/v3/relationship/{entityName}/{entityUrn}", "dataset", entityUrn)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    // Verify that slice options were not set
    OperationContext capturedOpContext = opContextCaptor.getValue();
    assertNull(capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions());
  }

  @Test
  public void testGetRelationshipsByEntityWithPitKeepAlive() throws Exception {
    String entityUrn = "urn:li:dataset:(urn:li:dataPlatform:testPlatform,test,PROD)";

    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, "test-scroll-id", Arrays.asList());

    when(mockGraphService.scrollRelatedEntities(
            any(), isNull(), any(), isNull(), any(), anySet(), any(), any(), isNull(), eq("10m"),
            anyInt(), isNull(), isNull()))
        .thenReturn(expectedResult);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/v3/relationship/{entityName}/{entityUrn}", "dataset", entityUrn)
                .param("pitKeepAlive", "10m")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful())
        .andExpect(jsonPath("$.scrollId").value("test-scroll-id"));
  }

  @Test
  public void testGetRelationshipsByEntityWithIncomingDirection() throws Exception {
    String entityUrn = "urn:li:dataset:(urn:li:dataPlatform:testPlatform,test,PROD)";

    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, "test-scroll-id", Arrays.asList());

    // Use ArgumentCaptor to verify the correct filter parameters for INCOMING
    ArgumentCaptor<Filter> sourceEntityFilterCaptor = ArgumentCaptor.forClass(Filter.class);
    ArgumentCaptor<Filter> destEntityFilterCaptor = ArgumentCaptor.forClass(Filter.class);

    when(mockGraphService.scrollRelatedEntities(
            any(),
            isNull(),
            sourceEntityFilterCaptor.capture(),
            isNull(),
            destEntityFilterCaptor.capture(),
            anySet(),
            any(),
            any(),
            isNull(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/v3/relationship/{entityName}/{entityUrn}", "dataset", entityUrn)
                .param("direction", "INCOMING")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful())
        .andExpect(jsonPath("$.scrollId").value("test-scroll-id"));

    // Verify INCOMING direction: sourceTypes=null, sourceEntityFilter=EMPTY, destTypes=null,
    // destEntityFilter=entityUrn
    assertNotNull(sourceEntityFilterCaptor.getValue());
    assertNotNull(destEntityFilterCaptor.getValue());
  }

  @Test
  public void testGetRelationshipsByEntityWithOutgoingDirection() throws Exception {
    String entityUrn = "urn:li:dataset:(urn:li:dataPlatform:testPlatform,test,PROD)";

    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, "test-scroll-id", Arrays.asList());

    // Use ArgumentCaptor to verify the correct filter parameters for OUTGOING
    ArgumentCaptor<Filter> sourceEntityFilterCaptor = ArgumentCaptor.forClass(Filter.class);
    ArgumentCaptor<Filter> destEntityFilterCaptor = ArgumentCaptor.forClass(Filter.class);

    when(mockGraphService.scrollRelatedEntities(
            any(),
            isNull(),
            sourceEntityFilterCaptor.capture(),
            isNull(),
            destEntityFilterCaptor.capture(),
            anySet(),
            any(),
            any(),
            isNull(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/v3/relationship/{entityName}/{entityUrn}", "dataset", entityUrn)
                .param("direction", "OUTGOING")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful())
        .andExpect(jsonPath("$.scrollId").value("test-scroll-id"));

    // Verify OUTGOING direction: sourceTypes=null, sourceEntityFilter=entityUrn, destTypes=null,
    // destEntityFilter=EMPTY
    assertNotNull(sourceEntityFilterCaptor.getValue());
    assertNotNull(destEntityFilterCaptor.getValue());
  }

  @Test
  public void testGetRelationshipsByEntityWithIncomingAndSpecificRelationshipTypes()
      throws Exception {
    String entityUrn = "urn:li:dataset:(urn:li:dataPlatform:testPlatform,test,PROD)";

    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, "test-scroll-id", Arrays.asList());

    ArgumentCaptor<Set> relationshipTypesCaptor = ArgumentCaptor.forClass(Set.class);

    when(mockGraphService.scrollRelatedEntities(
            any(),
            isNull(),
            any(),
            isNull(),
            any(),
            relationshipTypesCaptor.capture(),
            any(),
            any(),
            isNull(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/v3/relationship/{entityName}/{entityUrn}", "dataset", entityUrn)
                .param("direction", "INCOMING")
                .param("relationshipType[]", "DownstreamOf")
                .param("relationshipType[]", "Consumes")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    // Verify specific relationship types were passed
    Set capturedTypes = relationshipTypesCaptor.getValue();
    assertEquals(capturedTypes.size(), 2);
    assertTrue(capturedTypes.contains("DownstreamOf"));
    assertTrue(capturedTypes.contains("Consumes"));
  }

  @Test
  public void testGetRelationshipsByEntityWithOutgoingAndSpecificRelationshipTypes()
      throws Exception {
    String entityUrn = "urn:li:dataset:(urn:li:dataPlatform:testPlatform,test,PROD)";

    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, "test-scroll-id", Arrays.asList());

    ArgumentCaptor<Set> relationshipTypesCaptor = ArgumentCaptor.forClass(Set.class);

    when(mockGraphService.scrollRelatedEntities(
            any(),
            isNull(),
            any(),
            isNull(),
            any(),
            relationshipTypesCaptor.capture(),
            any(),
            any(),
            isNull(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/v3/relationship/{entityName}/{entityUrn}", "dataset", entityUrn)
                .param("direction", "OUTGOING")
                .param("relationshipType[]", "DownstreamOf")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    // Verify specific relationship type was passed
    Set capturedTypes = relationshipTypesCaptor.getValue();
    assertEquals(capturedTypes.size(), 1);
    assertTrue(capturedTypes.contains("DownstreamOf"));
  }

  @Test
  public void testGetRelationshipsByEntityWithEmptyRelationshipTypeFilter() throws Exception {
    String entityUrn = "urn:li:dataset:(urn:li:dataPlatform:testPlatform,test,PROD)";

    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, "test-scroll-id", Arrays.asList());

    ArgumentCaptor<Set> relationshipTypesCaptor = ArgumentCaptor.forClass(Set.class);

    when(mockGraphService.scrollRelatedEntities(
            any(),
            isNull(),
            any(),
            isNull(),
            any(),
            relationshipTypesCaptor.capture(),
            any(),
            any(),
            isNull(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/v3/relationship/{entityName}/{entityUrn}", "dataset", entityUrn)
                .param("direction", "OUTGOING")
                .param("relationshipType[]", "*")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    // Verify empty set was passed (wildcard "*" should result in empty set)
    Set capturedTypes = relationshipTypesCaptor.getValue();
    assertTrue(capturedTypes.isEmpty());
  }

  @Test
  public void testGetRelationshipsByEntityWithDefaultRelationshipTypeFilter() throws Exception {
    String entityUrn = "urn:li:dataset:(urn:li:dataPlatform:testPlatform,test,PROD)";

    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, "test-scroll-id", Arrays.asList());

    ArgumentCaptor<Set> relationshipTypesCaptor = ArgumentCaptor.forClass(Set.class);

    when(mockGraphService.scrollRelatedEntities(
            any(),
            isNull(),
            any(),
            isNull(),
            any(),
            relationshipTypesCaptor.capture(),
            any(),
            any(),
            isNull(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    // Don't pass relationshipType[] parameter - should default to "*"
    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/v3/relationship/{entityName}/{entityUrn}", "dataset", entityUrn)
                .param("direction", "INCOMING")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    // Verify empty set was passed (default "*" should result in empty set)
    Set capturedTypes = relationshipTypesCaptor.getValue();
    assertTrue(capturedTypes.isEmpty());
  }

  @Test
  public void testGetRelationshipsByEntityWithInvalidDirection() throws Exception {
    String entityUrn = "urn:li:dataset:(urn:li:dataPlatform:testPlatform,test,PROD)";

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/v3/relationship/{entityName}/{entityUrn}", "dataset", entityUrn)
                .param("direction", "INVALID")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is4xxClientError());
  }

  @Test
  public void testGetRelationshipsByEntityIncomingWithAllParameters() throws Exception {
    String entityUrn = "urn:li:dataset:(urn:li:dataPlatform:testPlatform,test,PROD)";

    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, "test-scroll-id", Arrays.asList());

    ArgumentCaptor<OperationContext> opContextCaptor =
        ArgumentCaptor.forClass(OperationContext.class);
    ArgumentCaptor<Filter> sourceEntityFilterCaptor = ArgumentCaptor.forClass(Filter.class);
    ArgumentCaptor<Filter> destEntityFilterCaptor = ArgumentCaptor.forClass(Filter.class);
    ArgumentCaptor<Set> relationshipTypesCaptor = ArgumentCaptor.forClass(Set.class);
    ArgumentCaptor<String> scrollIdCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> pitKeepAliveCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> countCaptor = ArgumentCaptor.forClass(Integer.class);

    when(mockGraphService.scrollRelatedEntities(
            opContextCaptor.capture(),
            isNull(),
            sourceEntityFilterCaptor.capture(),
            isNull(),
            destEntityFilterCaptor.capture(),
            relationshipTypesCaptor.capture(),
            any(),
            any(),
            scrollIdCaptor.capture(),
            pitKeepAliveCaptor.capture(),
            countCaptor.capture(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/v3/relationship/{entityName}/{entityUrn}", "dataset", entityUrn)
                .param("direction", "INCOMING")
                .param("relationshipType[]", "DownstreamOf")
                .param("relationshipType[]", "Consumes")
                .param("count", "20")
                .param("scrollId", "prev-scroll-id")
                .param("pitKeepAlive", "15m")
                .param("sliceId", "2")
                .param("sliceMax", "5")
                .param("includeSoftDelete", "true")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful())
        .andExpect(jsonPath("$.scrollId").value("test-scroll-id"));

    // Verify all parameters were correctly passed
    OperationContext capturedOpContext = opContextCaptor.getValue();
    assertNotNull(capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions());
    assertEquals(
        capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions().getId().intValue(),
        2);
    assertEquals(
        capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions().getMax().intValue(),
        5);
    assertTrue(capturedOpContext.getSearchContext().getSearchFlags().isIncludeSoftDeleted());

    // Verify INCOMING direction parameters
    assertNotNull(sourceEntityFilterCaptor.getValue());
    assertNotNull(destEntityFilterCaptor.getValue());

    // Verify relationship types
    Set capturedTypes = relationshipTypesCaptor.getValue();
    assertEquals(capturedTypes.size(), 2);
    assertTrue(capturedTypes.contains("DownstreamOf"));
    assertTrue(capturedTypes.contains("Consumes"));

    // Verify other parameters
    assertEquals(scrollIdCaptor.getValue(), "prev-scroll-id");
    assertEquals(pitKeepAliveCaptor.getValue(), "15m");
    assertEquals(countCaptor.getValue().intValue(), 20);
  }

  @Test
  public void testGetRelationshipsByEntityOutgoingWithAllParameters() throws Exception {
    String entityUrn = "urn:li:dataset:(urn:li:dataPlatform:testPlatform,test,PROD)";

    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, "test-scroll-id", Arrays.asList());

    ArgumentCaptor<OperationContext> opContextCaptor =
        ArgumentCaptor.forClass(OperationContext.class);
    ArgumentCaptor<Filter> sourceEntityFilterCaptor = ArgumentCaptor.forClass(Filter.class);
    ArgumentCaptor<Filter> destEntityFilterCaptor = ArgumentCaptor.forClass(Filter.class);
    ArgumentCaptor<Set> relationshipTypesCaptor = ArgumentCaptor.forClass(Set.class);

    when(mockGraphService.scrollRelatedEntities(
            opContextCaptor.capture(),
            isNull(),
            sourceEntityFilterCaptor.capture(),
            isNull(),
            destEntityFilterCaptor.capture(),
            relationshipTypesCaptor.capture(),
            any(),
            any(),
            any(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/v3/relationship/{entityName}/{entityUrn}", "dataset", entityUrn)
                .param("direction", "OUTGOING")
                .param("relationshipType[]", "Produces")
                .param("count", "25")
                .param("sliceId", "1")
                .param("sliceMax", "4")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    // Verify OUTGOING direction parameters
    assertNotNull(sourceEntityFilterCaptor.getValue());
    assertNotNull(destEntityFilterCaptor.getValue());

    // Verify relationship types
    Set capturedTypes = relationshipTypesCaptor.getValue();
    assertEquals(capturedTypes.size(), 1);
    assertTrue(capturedTypes.contains("Produces"));

    // Verify slice options
    OperationContext capturedOpContext = opContextCaptor.getValue();
    assertNotNull(capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions());
    assertEquals(
        capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions().getId().intValue(),
        1);
    assertEquals(
        capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions().getMax().intValue(),
        4);
  }

  @Test
  public void testGetRelationshipsByEntityWithEmptyPitKeepAlive() throws Exception {
    String entityUrn = "urn:li:dataset:(urn:li:dataPlatform:testPlatform,test,PROD)";

    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, "test-scroll-id", Arrays.asList());

    ArgumentCaptor<String> pitKeepAliveCaptor = ArgumentCaptor.forClass(String.class);

    when(mockGraphService.scrollRelatedEntities(
            any(),
            isNull(),
            any(),
            isNull(),
            any(),
            anySet(),
            any(),
            any(),
            isNull(),
            pitKeepAliveCaptor.capture(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/v3/relationship/{entityName}/{entityUrn}", "dataset", entityUrn)
                .param("direction", "OUTGOING")
                .param("pitKeepAlive", "")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    // Verify that empty pitKeepAlive is converted to null
    assertNotNull(pitKeepAliveCaptor.getValue());
  }

  // -------------------------------------------------------------------------
  // scrollRelationships tests
  // -------------------------------------------------------------------------

  private static final ScrollRelationshipsRequestBody EMPTY_SCROLL_BODY =
      ScrollRelationshipsRequestBody.builder().build();

  @Test
  public void testScrollRelationshipsDefaults() throws Exception {
    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, "scroll-1", Arrays.asList());

    ArgumentCaptor<GraphFilters> graphFiltersCaptor = ArgumentCaptor.forClass(GraphFilters.class);

    when(mockGraphService.scrollRelatedEntities(
            any(),
            graphFiltersCaptor.capture(),
            any(),
            isNull(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/relationship/scroll")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(EMPTY_SCROLL_BODY))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful())
        .andExpect(jsonPath("$.scrollId").value("scroll-1"));

    GraphFilters captured = graphFiltersCaptor.getValue();
    // No relationshipTypes param → null → empty set (all types)
    assertTrue(captured.getRelationshipTypes().isEmpty());
    // No sourceType / destinationType → null passed through
    assertNull(captured.getSourceTypes());
    assertNull(captured.getDestinationTypes());
    // No filters in body → EMPTY_FILTER (no criteria)
    assertTrue(
        captured.getSourceEntityFilter().getOr().isEmpty()
            || captured.getSourceEntityFilter().getOr().stream()
                .allMatch(cc -> cc.getAnd().isEmpty()));
    assertTrue(
        captured.getDestinationEntityFilter().getOr().isEmpty()
            || captured.getDestinationEntityFilter().getOr().stream()
                .allMatch(cc -> cc.getAnd().isEmpty()));
  }

  @Test
  public void testScrollRelationshipsWithRelationshipTypes() throws Exception {
    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, null, Arrays.asList());

    ArgumentCaptor<GraphFilters> graphFiltersCaptor = ArgumentCaptor.forClass(GraphFilters.class);

    when(mockGraphService.scrollRelatedEntities(
            any(),
            graphFiltersCaptor.capture(),
            any(),
            isNull(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/relationship/scroll")
                .param("relationshipTypes", "DownstreamOf")
                .param("relationshipTypes", "Consumes")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(EMPTY_SCROLL_BODY))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    Set<String> capturedTypes = graphFiltersCaptor.getValue().getRelationshipTypes();
    assertEquals(capturedTypes.size(), 2);
    assertTrue(capturedTypes.contains("DownstreamOf"));
    assertTrue(capturedTypes.contains("Consumes"));
  }

  @Test
  public void testScrollRelationshipsWithEntityTypeFilters() throws Exception {
    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, null, Arrays.asList());

    ArgumentCaptor<GraphFilters> graphFiltersCaptor = ArgumentCaptor.forClass(GraphFilters.class);

    when(mockGraphService.scrollRelatedEntities(
            any(),
            graphFiltersCaptor.capture(),
            any(),
            isNull(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/relationship/scroll")
                .param("sourceTypes", "dataset")
                .param("destinationTypes", "chart")
                .param("destinationTypes", "dashboard")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(EMPTY_SCROLL_BODY))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    GraphFilters captured = graphFiltersCaptor.getValue();
    Set<String> capturedSrcTypes = captured.getSourceTypes();
    assertEquals(capturedSrcTypes.size(), 1);
    assertTrue(capturedSrcTypes.contains("dataset"));

    Set<String> capturedDstTypes = captured.getDestinationTypes();
    assertEquals(capturedDstTypes.size(), 2);
    assertTrue(capturedDstTypes.contains("chart"));
    assertTrue(capturedDstTypes.contains("dashboard"));
  }

  @Test
  public void testScrollRelationshipsWithUrnFilters() throws Exception {
    String sourceUrn = "urn:li:dataset:(urn:li:dataPlatform:testPlatform,src,PROD)";
    String destUrn1 = "urn:li:chart:(looker,chart1)";
    String destUrn2 = "urn:li:chart:(looker,chart2)";

    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, null, Arrays.asList());

    ArgumentCaptor<GraphFilters> graphFiltersCaptor = ArgumentCaptor.forClass(GraphFilters.class);

    when(mockGraphService.scrollRelatedEntities(
            any(),
            graphFiltersCaptor.capture(),
            any(),
            isNull(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    io.datahubproject.openapi.v3.models.Filter sourceFilter =
        io.datahubproject.openapi.v3.models.Filter.builder()
            .and(
                List.of(
                    ConjunctiveCriterion.builder()
                        .criteria(
                            List.of(
                                Criterion.builder()
                                    .field("urn")
                                    .values(List.of(sourceUrn))
                                    .condition(Criterion.Condition.EQUAL)
                                    .build()))
                        .build()))
            .build();
    io.datahubproject.openapi.v3.models.Filter destFilter =
        io.datahubproject.openapi.v3.models.Filter.builder()
            .and(
                List.of(
                    ConjunctiveCriterion.builder()
                        .criteria(
                            List.of(
                                Criterion.builder()
                                    .field("urn")
                                    .values(List.of(destUrn1, destUrn2))
                                    .condition(Criterion.Condition.EQUAL)
                                    .build()))
                        .build()))
            .build();

    ScrollRelationshipsRequestBody body =
        ScrollRelationshipsRequestBody.builder()
            .sourceFilter(sourceFilter)
            .destinationFilter(destFilter)
            .build();

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/relationship/scroll")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(body))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    GraphFilters captured = graphFiltersCaptor.getValue();
    // Source filter should have a non-empty criterion on the "urn" field
    Filter capturedSrcFilter = captured.getSourceEntityFilter();
    assertNotNull(capturedSrcFilter);
    assertFalse(capturedSrcFilter.getOr().isEmpty());
    assertEquals(capturedSrcFilter.getOr().get(0).getAnd().get(0).getField(), "urn");
    assertFalse(capturedSrcFilter.getOr().get(0).getAnd().get(0).getValues().isEmpty());

    // Destination filter should have a non-empty criterion on the "urn" field with both URNs
    Filter capturedDstFilter = captured.getDestinationEntityFilter();
    assertNotNull(capturedDstFilter);
    assertFalse(capturedDstFilter.getOr().isEmpty());
    assertEquals(capturedDstFilter.getOr().get(0).getAnd().get(0).getField(), "urn");
    assertEquals(capturedDstFilter.getOr().get(0).getAnd().get(0).getValues().size(), 2);
  }

  @Test
  public void testScrollRelationshipsWithSliceOptions() throws Exception {
    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, null, Arrays.asList());

    ArgumentCaptor<OperationContext> opContextCaptor =
        ArgumentCaptor.forClass(OperationContext.class);

    when(mockGraphService.scrollRelatedEntities(
            opContextCaptor.capture(),
            any(GraphFilters.class),
            any(),
            isNull(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/relationship/scroll")
                .param("sliceId", "1")
                .param("sliceMax", "4")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(EMPTY_SCROLL_BODY))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    OperationContext capturedOpContext = opContextCaptor.getValue();
    assertNotNull(capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions());
    assertEquals(
        capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions().getId().intValue(),
        1);
    assertEquals(
        capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions().getMax().intValue(),
        4);
  }

  @Test
  public void testScrollRelationshipsAllParameters() throws Exception {
    String sourceUrn = "urn:li:dataset:(urn:li:dataPlatform:testPlatform,src,PROD)";
    String destUrn = "urn:li:chart:(looker,chart1)";

    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(5, 20, "next-scroll", Arrays.asList());

    ArgumentCaptor<OperationContext> opContextCaptor =
        ArgumentCaptor.forClass(OperationContext.class);
    ArgumentCaptor<GraphFilters> graphFiltersCaptor = ArgumentCaptor.forClass(GraphFilters.class);
    ArgumentCaptor<String> scrollIdCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> pitCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> countCaptor = ArgumentCaptor.forClass(Integer.class);

    when(mockGraphService.scrollRelatedEntities(
            opContextCaptor.capture(),
            graphFiltersCaptor.capture(),
            any(),
            scrollIdCaptor.capture(),
            pitCaptor.capture(),
            countCaptor.capture(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    io.datahubproject.openapi.v3.models.Filter sourceFilter =
        io.datahubproject.openapi.v3.models.Filter.builder()
            .and(
                List.of(
                    ConjunctiveCriterion.builder()
                        .criteria(
                            List.of(
                                Criterion.builder()
                                    .field("urn")
                                    .values(List.of(sourceUrn))
                                    .condition(Criterion.Condition.EQUAL)
                                    .build()))
                        .build()))
            .build();
    io.datahubproject.openapi.v3.models.Filter destFilter =
        io.datahubproject.openapi.v3.models.Filter.builder()
            .and(
                List.of(
                    ConjunctiveCriterion.builder()
                        .criteria(
                            List.of(
                                Criterion.builder()
                                    .field("urn")
                                    .values(List.of(destUrn))
                                    .condition(Criterion.Condition.EQUAL)
                                    .build()))
                        .build()))
            .build();
    io.datahubproject.openapi.v3.models.Filter edgeFilter =
        io.datahubproject.openapi.v3.models.Filter.builder()
            .and(
                List.of(
                    ConjunctiveCriterion.builder()
                        .criteria(
                            List.of(
                                Criterion.builder()
                                    .field("relationshipType")
                                    .values(List.of("DownstreamOf"))
                                    .condition(Criterion.Condition.EQUAL)
                                    .build()))
                        .build()))
            .build();

    ScrollRelationshipsRequestBody body =
        ScrollRelationshipsRequestBody.builder()
            .sourceFilter(sourceFilter)
            .destinationFilter(destFilter)
            .edgeFilter(edgeFilter)
            .build();

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/relationship/scroll")
                .param("relationshipTypes", "DownstreamOf")
                .param("sourceTypes", "dataset")
                .param("destinationTypes", "chart")
                .param("count", "20")
                .param("scrollId", "prev-scroll")
                .param("pitKeepAlive", "10m")
                .param("sliceId", "0")
                .param("sliceMax", "3")
                .param("includeSoftDelete", "true")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(body))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful())
        .andExpect(jsonPath("$.scrollId").value("next-scroll"));

    GraphFilters captured = graphFiltersCaptor.getValue();

    // Verify relationship types
    assertEquals(captured.getRelationshipTypes().size(), 1);
    assertTrue(captured.getRelationshipTypes().contains("DownstreamOf"));

    // Verify entity type filters
    assertEquals(captured.getSourceTypes().size(), 1);
    assertTrue(captured.getSourceTypes().contains("dataset"));
    assertEquals(captured.getDestinationTypes().size(), 1);
    assertTrue(captured.getDestinationTypes().contains("chart"));

    // Verify URN filters from request body
    assertFalse(captured.getSourceEntityFilter().getOr().isEmpty());
    assertFalse(captured.getDestinationEntityFilter().getOr().isEmpty());

    // Verify edge filter was embedded in the RelationshipFilter
    RelationshipFilter capturedRelFilter = captured.getRelationshipFilter();
    assertNotNull(capturedRelFilter);
    assertFalse(capturedRelFilter.getOr().isEmpty());
    assertEquals(capturedRelFilter.getOr().get(0).getAnd().get(0).getField(), "relationshipType");

    // Verify pagination parameters
    assertEquals(scrollIdCaptor.getValue(), "prev-scroll");
    assertEquals(pitCaptor.getValue(), "10m");
    assertEquals(countCaptor.getValue().intValue(), 20);

    // Verify slice options and includeSoftDelete
    OperationContext capturedOpContext = opContextCaptor.getValue();
    assertNotNull(capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions());
    assertEquals(
        capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions().getId().intValue(),
        0);
    assertEquals(
        capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions().getMax().intValue(),
        3);
    assertTrue(capturedOpContext.getSearchContext().getSearchFlags().isIncludeSoftDeleted());

    // Verify no lineage triplets on the regular scroll endpoint
    assertNull(captured.getAllowedEdgeTriplets());
  }

  @Test
  public void testScrollRelationshipsDefaultDirectionIsOutgoing() throws Exception {
    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, "scroll-1", Arrays.asList());

    ArgumentCaptor<GraphFilters> graphFiltersCaptor = ArgumentCaptor.forClass(GraphFilters.class);

    when(mockGraphService.scrollRelatedEntities(
            any(),
            graphFiltersCaptor.capture(),
            any(),
            isNull(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/relationship/scroll")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(EMPTY_SCROLL_BODY))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    assertEquals(
        graphFiltersCaptor.getValue().getRelationshipFilter().getDirection(),
        RelationshipDirection.OUTGOING);
  }

  @Test
  public void testScrollRelationshipsWithDirectionOutgoing() throws Exception {
    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, "scroll-out", Arrays.asList());

    ArgumentCaptor<GraphFilters> graphFiltersCaptor = ArgumentCaptor.forClass(GraphFilters.class);

    when(mockGraphService.scrollRelatedEntities(
            any(),
            graphFiltersCaptor.capture(),
            any(),
            isNull(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/relationship/scroll")
                .param("direction", "OUTGOING")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(EMPTY_SCROLL_BODY))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful())
        .andExpect(jsonPath("$.scrollId").value("scroll-out"));

    assertEquals(
        graphFiltersCaptor.getValue().getRelationshipFilter().getDirection(),
        RelationshipDirection.OUTGOING);
  }

  @Test
  public void testScrollRelationshipsWithDirectionIncoming() throws Exception {
    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, "scroll-in", Arrays.asList());

    ArgumentCaptor<GraphFilters> graphFiltersCaptor = ArgumentCaptor.forClass(GraphFilters.class);

    when(mockGraphService.scrollRelatedEntities(
            any(),
            graphFiltersCaptor.capture(),
            any(),
            isNull(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/relationship/scroll")
                .param("direction", "INCOMING")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(EMPTY_SCROLL_BODY))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful())
        .andExpect(jsonPath("$.scrollId").value("scroll-in"));

    assertEquals(
        graphFiltersCaptor.getValue().getRelationshipFilter().getDirection(),
        RelationshipDirection.INCOMING);
  }

  @Test
  public void testScrollRelationshipsWithDirectionCaseInsensitive() throws Exception {
    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, null, Arrays.asList());

    ArgumentCaptor<GraphFilters> graphFiltersCaptor = ArgumentCaptor.forClass(GraphFilters.class);

    when(mockGraphService.scrollRelatedEntities(
            any(),
            graphFiltersCaptor.capture(),
            any(),
            isNull(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/relationship/scroll")
                .param("direction", "outgoing")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(EMPTY_SCROLL_BODY))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    assertEquals(
        graphFiltersCaptor.getValue().getRelationshipFilter().getDirection(),
        RelationshipDirection.OUTGOING);
  }

  @Test
  public void testScrollRelationshipsWithInvalidDirection() throws Exception {
    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/relationship/scroll")
                .param("direction", "SIDEWAYS")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(EMPTY_SCROLL_BODY))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is4xxClientError());
  }

  @Test
  public void testScrollLineageSetsAllowedEdgeTriplets() throws Exception {
    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, "lineage-scroll", Arrays.asList());

    // Setup lineage registry on the mock graph service
    LineageRegistry lineageRegistry =
        TestOperationContexts.systemContextNoSearchAuthorization().getLineageRegistry();
    when(mockGraphService.getLineageRegistry()).thenReturn(lineageRegistry);

    ArgumentCaptor<GraphFilters> graphFiltersCaptor = ArgumentCaptor.forClass(GraphFilters.class);

    when(mockGraphService.scrollRelatedEntities(
            any(),
            graphFiltersCaptor.capture(),
            any(),
            isNull(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/lineage/scroll")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(EMPTY_SCROLL_BODY))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful())
        .andExpect(jsonPath("$.scrollId").value("lineage-scroll"));

    GraphFilters captured = graphFiltersCaptor.getValue();
    // scrollLineage should populate allowedEdgeTriplets from the lineage registry
    assertNotNull(captured.getAllowedEdgeTriplets());
    assertFalse(captured.getAllowedEdgeTriplets().isEmpty());

    // Verify a known lineage edge is present (dataset DownstreamOf dataset)
    boolean hasDownstreamOf =
        captured.getAllowedEdgeTriplets().stream()
            .anyMatch(
                p -> p.getKey().equals("dataset") && p.getValue().getType().equals("DownstreamOf"));
    assertTrue(hasDownstreamOf, "Expected lineage triplets to include dataset DownstreamOf");
  }

  @Test
  public void testScrollLineageWithAdditionalFilters() throws Exception {
    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, null, Arrays.asList());

    LineageRegistry lineageRegistry =
        TestOperationContexts.systemContextNoSearchAuthorization().getLineageRegistry();
    when(mockGraphService.getLineageRegistry()).thenReturn(lineageRegistry);

    ArgumentCaptor<GraphFilters> graphFiltersCaptor = ArgumentCaptor.forClass(GraphFilters.class);

    when(mockGraphService.scrollRelatedEntities(
            any(),
            graphFiltersCaptor.capture(),
            any(),
            isNull(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/lineage/scroll")
                .param("sourceTypes", "dataset")
                .param("destinationTypes", "chart")
                .param("relationshipTypes", "Consumes")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(EMPTY_SCROLL_BODY))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    GraphFilters captured = graphFiltersCaptor.getValue();
    // Should have both the request-level filters AND the lineage triplets
    assertEquals(Set.of("dataset"), captured.getSourceTypes());
    assertEquals(Set.of("chart"), captured.getDestinationTypes());
    assertEquals(Set.of("Consumes"), captured.getRelationshipTypes());
    assertNotNull(captured.getAllowedEdgeTriplets());
    assertFalse(captured.getAllowedEdgeTriplets().isEmpty());
  }

  @Test
  public void testScrollEndpointDoesNotSetTriplets() throws Exception {
    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, null, Arrays.asList());

    ArgumentCaptor<GraphFilters> graphFiltersCaptor = ArgumentCaptor.forClass(GraphFilters.class);

    when(mockGraphService.scrollRelatedEntities(
            any(),
            graphFiltersCaptor.capture(),
            any(),
            isNull(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/relationship/scroll")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(EMPTY_SCROLL_BODY))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    // Regular scroll should NOT set triplets
    assertNull(graphFiltersCaptor.getValue().getAllowedEdgeTriplets());
  }
}
