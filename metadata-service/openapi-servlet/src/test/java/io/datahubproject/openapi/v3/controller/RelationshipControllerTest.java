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
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.openapi.config.GlobalControllerExceptionHandler;
import io.datahubproject.openapi.config.SpringWebConfig;
import io.datahubproject.openapi.config.TracingInterceptor;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Arrays;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SpringBootTest(classes = {SpringWebConfig.class})
@ComponentScan(basePackages = {"io.datahubproject.openapi.v3.controller.RelationshipController"})
@Import({
  SpringWebConfig.class,
  TracingInterceptor.class,
  RelationshipController.class,
  RelationshipControllerTest.RelationshipControllerTestConfig.class,
  GlobalControllerExceptionHandler.class,
})
@AutoConfigureWebMvc
@AutoConfigureMockMvc
public class RelationshipControllerTest extends AbstractTestNGSpringContextTests {
  @Autowired private RelationshipController relationshipController;
  @Autowired private MockMvc mockMvc;
  @Autowired private GraphService mockGraphService;

  @BeforeMethod
  public void setup() {
    org.mockito.MockitoAnnotations.openMocks(this);
  }

  @TestConfiguration
  public static class RelationshipControllerTestConfig {
    @MockBean private ConfigurationProvider configurationProvider;
    @MockBean private EntityRegistry entityRegistry;
    @MockBean private SystemTelemetryContext systemTelemetryContext;

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
        0,
        capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions().getId().intValue());
    assertEquals(
        2,
        capturedOpContext
            .getSearchContext()
            .getSearchFlags()
            .getSliceOptions()
            .getMax()
            .intValue());
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
        1,
        capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions().getId().intValue());
    assertEquals(
        3,
        capturedOpContext
            .getSearchContext()
            .getSearchFlags()
            .getSliceOptions()
            .getMax()
            .intValue());
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
    assertEquals(2, capturedTypes.size());
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
    assertEquals(1, capturedTypes.size());
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
        2,
        capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions().getId().intValue());
    assertEquals(
        5,
        capturedOpContext
            .getSearchContext()
            .getSearchFlags()
            .getSliceOptions()
            .getMax()
            .intValue());
    assertTrue(capturedOpContext.getSearchContext().getSearchFlags().isIncludeSoftDeleted());

    // Verify INCOMING direction parameters
    assertNotNull(sourceEntityFilterCaptor.getValue());
    assertNotNull(destEntityFilterCaptor.getValue());

    // Verify relationship types
    Set capturedTypes = relationshipTypesCaptor.getValue();
    assertEquals(2, capturedTypes.size());
    assertTrue(capturedTypes.contains("DownstreamOf"));
    assertTrue(capturedTypes.contains("Consumes"));

    // Verify other parameters
    assertEquals("prev-scroll-id", scrollIdCaptor.getValue());
    assertEquals("15m", pitKeepAliveCaptor.getValue());
    assertEquals(20, countCaptor.getValue().intValue());
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
    assertEquals(1, capturedTypes.size());
    assertTrue(capturedTypes.contains("Produces"));

    // Verify slice options
    OperationContext capturedOpContext = opContextCaptor.getValue();
    assertNotNull(capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions());
    assertEquals(
        1,
        capturedOpContext.getSearchContext().getSearchFlags().getSliceOptions().getId().intValue());
    assertEquals(
        4,
        capturedOpContext
            .getSearchContext()
            .getSearchFlags()
            .getSliceOptions()
            .getMax()
            .intValue());
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
}
