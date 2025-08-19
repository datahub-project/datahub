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
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.graph.GraphService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.openapi.config.GlobalControllerExceptionHandler;
import io.datahubproject.openapi.config.SpringWebConfig;
import io.datahubproject.openapi.config.TracingInterceptor;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Arrays;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SpringBootTest(classes = {SpringWebConfig.class})
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

  @Autowired
  @Qualifier("systemOperationContext")
  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    org.mockito.MockitoAnnotations.openMocks(this);
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
            isNull(),
            isNull(),
            isNull(),
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
            isNull(),
            isNull(),
            isNull(),
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
            isNull(),
            isNull(),
            isNull(),
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
            isNull(),
            isNull(),
            isNull(),
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
            isNull(),
            isNull(),
            isNull(),
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
            any(), isNull(), isNull(), isNull(), isNull(), anySet(), any(), any(), isNull(),
            eq("10m"), anyInt(), isNull(), isNull()))
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

  @TestConfiguration
  public static class RelationshipControllerTestConfig {

    @Bean(name = "systemOperationContext")
    public OperationContext systemOperationContext() {
      return TestOperationContexts.systemContextNoSearchAuthorization();
    }

    @MockBean public GraphService graphService;

    @Bean
    @Primary
    public SystemTelemetryContext systemTelemetryContext(
        @Qualifier("systemOperationContext") OperationContext systemOperationContext) {
      return systemOperationContext.getSystemTelemetryContext();
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

    @MockBean private ConfigurationProvider configurationProvider;
  }
}
