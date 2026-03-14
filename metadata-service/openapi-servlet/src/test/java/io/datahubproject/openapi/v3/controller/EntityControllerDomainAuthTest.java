package io.datahubproject.openapi.v3.controller;

import static org.mockito.ArgumentMatchers.*;
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
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entity.versioning.EntityVersioningServiceFactory;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.UpdateAspectResult;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.openapi.config.GlobalControllerExceptionHandler;
import io.datahubproject.openapi.config.SpringWebConfig;
import io.datahubproject.openapi.config.TracingInterceptor;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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

/**
 * Tests for EntityController domain-based authorization paths. These tests focus on the
 * domain-based authorization logic in patchEntity and createGenericEntities.
 */
@SpringBootTest(classes = {SpringWebConfig.class})
@ComponentScan(basePackages = {"io.datahubproject.openapi.v3.controller.EntityController"})
@Import({
  SpringWebConfig.class,
  TracingInterceptor.class,
  EntityController.class,
  EntityControllerDomainAuthTest.EntityControllerDomainAuthTestConfig.class,
  EntityVersioningServiceFactory.class,
  GlobalControllerExceptionHandler.class,
})
@AutoConfigureWebMvc
@AutoConfigureMockMvc
public class EntityControllerDomainAuthTest extends AbstractTestNGSpringContextTests {
  @Autowired private EntityController entityController;
  @Autowired private MockMvc mockMvc;
  @Autowired private EntityService<?> mockEntityService;
  @Autowired private ConfigurationProvider configurationProvider;
  @Autowired private FeatureFlags mockFeatureFlags;

  @BeforeMethod
  public void setup() {
    org.mockito.MockitoAnnotations.openMocks(this);
  }

  @Test
  public void initTest() {
    assertNotNull(entityController);
  }

  @Test
  public void testPatchEntityWithDomainBasedAuth() throws Exception {
    Urn TEST_URN = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:testPlatform,1,PROD)");

    // Enable domain-based authorization
    when(configurationProvider.getFeatureFlags()).thenReturn(mockFeatureFlags);
    when(mockFeatureFlags.isDomainBasedAuthorizationEnabled()).thenReturn(true);

    // Mock entity service response for patch
    when(mockEntityService.ingestProposal(
            any(OperationContext.class), any(AspectsBatch.class), eq(false)))
        .thenAnswer(
            invocation -> {
              AspectsBatch batch = invocation.getArgument(1);
              List<IngestResult> results = new ArrayList<>();
              for (MCPItem item : batch.getMCPItems()) {
                IngestResult result =
                    IngestResult.builder()
                        .urn(item.getUrn())
                        .request(item)
                        .result(
                            UpdateAspectResult.builder()
                                .urn(item.getUrn())
                                .auditStamp(item.getAuditStamp())
                                .newValue(new Status().setRemoved(false))
                                .newSystemMetadata(new SystemMetadata())
                                .build())
                        .sqlCommitted(true)
                        .isUpdate(true)
                        .publishedMCL(true)
                        .build();
                results.add(result);
              }
              return results;
            });

    // Mock entity exists check for domain validation
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), eq(false)))
        .thenReturn(true);

    String patchBody =
        "[\n"
            + "    {\n"
            + "      \"urn\": \"urn:li:dataset:(urn:li:dataPlatform:testPlatform,1,PROD)\",\n"
            + "      \"status\": {\n"
            + "        \"value\": {\n"
            + "          \"patch\": [{\n"
            + "            \"op\": \"replace\",\n"
            + "            \"path\": \"/removed\",\n"
            + "            \"value\": false\n"
            + "          }]\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "]";

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch("/openapi/v3/entity/dataset")
                .content(patchBody)
                .contentType("application/json-patch+json")
                .param("async", "false")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful())
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].urn").value(TEST_URN.toString()));
  }

  @Test
  public void testCreateGenericEntitiesWithDomainBasedAuth() throws Exception {
    Urn TEST_URN = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:testPlatform,1,PROD)");

    // Enable domain-based authorization
    when(configurationProvider.getFeatureFlags()).thenReturn(mockFeatureFlags);
    when(mockFeatureFlags.isDomainBasedAuthorizationEnabled()).thenReturn(true);

    // Mock entity service response
    when(mockEntityService.ingestProposal(
            any(OperationContext.class), any(AspectsBatch.class), eq(false)))
        .thenAnswer(
            invocation -> {
              AspectsBatch batch = invocation.getArgument(1);
              List<IngestResult> results = new ArrayList<>();
              for (MCPItem item : batch.getMCPItems()) {
                IngestResult result =
                    IngestResult.builder()
                        .urn(item.getUrn())
                        .request(item)
                        .result(
                            UpdateAspectResult.builder()
                                .urn(item.getUrn())
                                .auditStamp(item.getAuditStamp())
                                .newValue(new Status().setRemoved(false))
                                .newSystemMetadata(new SystemMetadata())
                                .build())
                        .sqlCommitted(true)
                        .build();
                results.add(result);
              }
              return results;
            });

    // Mock entity exists check for domain validation
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), eq(false)))
        .thenReturn(true);

    String requestBody =
        "{\n"
            + "  \"dataset\": [\n"
            + "    {\n"
            + "      \"urn\": \"urn:li:dataset:(urn:li:dataPlatform:testPlatform,1,PROD)\",\n"
            + "      \"status\": {\n"
            + "        \"value\": {\n"
            + "          \"removed\": false\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  ]\n"
            + "}";

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/entity/generic")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON)
                .param("async", "false")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful())
        .andExpect(MockMvcResultMatchers.jsonPath("$.dataset[0].urn").value(TEST_URN.toString()));
  }

  @Test
  public void testPatchEntityWithDomainBasedAuthDisabled() throws Exception {
    Urn TEST_URN = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:testPlatform,2,PROD)");

    // Disable domain-based authorization
    when(configurationProvider.getFeatureFlags()).thenReturn(mockFeatureFlags);
    when(mockFeatureFlags.isDomainBasedAuthorizationEnabled()).thenReturn(false);

    // Mock entity service response for patch
    when(mockEntityService.ingestProposal(
            any(OperationContext.class), any(AspectsBatch.class), eq(false)))
        .thenAnswer(
            invocation -> {
              AspectsBatch batch = invocation.getArgument(1);
              List<IngestResult> results = new ArrayList<>();
              for (MCPItem item : batch.getMCPItems()) {
                IngestResult result =
                    IngestResult.builder()
                        .urn(item.getUrn())
                        .request(item)
                        .result(
                            UpdateAspectResult.builder()
                                .urn(item.getUrn())
                                .auditStamp(item.getAuditStamp())
                                .newValue(new Status().setRemoved(false))
                                .newSystemMetadata(new SystemMetadata())
                                .build())
                        .sqlCommitted(true)
                        .isUpdate(true)
                        .publishedMCL(true)
                        .build();
                results.add(result);
              }
              return results;
            });

    String patchBody =
        "[\n"
            + "    {\n"
            + "      \"urn\": \"urn:li:dataset:(urn:li:dataPlatform:testPlatform,2,PROD)\",\n"
            + "      \"status\": {\n"
            + "        \"value\": {\n"
            + "          \"patch\": [{\n"
            + "            \"op\": \"replace\",\n"
            + "            \"path\": \"/removed\",\n"
            + "            \"value\": false\n"
            + "          }]\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "]";

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch("/openapi/v3/entity/dataset")
                .content(patchBody)
                .contentType("application/json-patch+json")
                .param("async", "false")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful())
        .andExpect(MockMvcResultMatchers.jsonPath("$[0].urn").value(TEST_URN.toString()));
  }

  @Test
  public void testCreateGenericEntitiesWithNullFeatureFlags() throws Exception {
    Urn TEST_URN = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:testPlatform,3,PROD)");

    // Set feature flags to null
    when(configurationProvider.getFeatureFlags()).thenReturn(null);

    // Mock entity service response
    when(mockEntityService.ingestProposal(
            any(OperationContext.class), any(AspectsBatch.class), eq(false)))
        .thenAnswer(
            invocation -> {
              AspectsBatch batch = invocation.getArgument(1);
              List<IngestResult> results = new ArrayList<>();
              for (MCPItem item : batch.getMCPItems()) {
                IngestResult result =
                    IngestResult.builder()
                        .urn(item.getUrn())
                        .request(item)
                        .result(
                            UpdateAspectResult.builder()
                                .urn(item.getUrn())
                                .auditStamp(item.getAuditStamp())
                                .newValue(new Status().setRemoved(false))
                                .newSystemMetadata(new SystemMetadata())
                                .build())
                        .sqlCommitted(true)
                        .build();
                results.add(result);
              }
              return results;
            });

    String requestBody =
        "{\n"
            + "  \"dataset\": [\n"
            + "    {\n"
            + "      \"urn\": \"urn:li:dataset:(urn:li:dataPlatform:testPlatform,3,PROD)\",\n"
            + "      \"status\": {\n"
            + "        \"value\": {\n"
            + "          \"removed\": false\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  ]\n"
            + "}";

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/entity/generic")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON)
                .param("async", "false")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful())
        .andExpect(MockMvcResultMatchers.jsonPath("$.dataset[0].urn").value(TEST_URN.toString()));
  }

  @TestConfiguration
  public static class EntityControllerDomainAuthTestConfig {
    @MockBean public EntityServiceImpl entityService;
    @MockBean public SearchService searchService;
    @MockBean public TimeseriesAspectService timeseriesAspectService;
    @MockBean public SystemTelemetryContext systemTelemetryContext;

    @Bean
    public ObjectMapper objectMapper() {
      return new ObjectMapper();
    }

    @Bean
    public FeatureFlags featureFlags() {
      return mock(FeatureFlags.class);
    }

    @Bean(name = "systemOperationContext")
    public OperationContext systemOperationContext() {
      return TestOperationContexts.systemContextNoSearchAuthorization();
    }

    @Bean("entityRegistry")
    @Primary
    public EntityRegistry entityRegistry(
        @Qualifier("systemOperationContext") final OperationContext testOperationContext) {
      return testOperationContext.getEntityRegistry();
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

    @Bean
    @Primary
    public ConfigurationProvider configurationProvider(FeatureFlags featureFlags) {
      ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
      when(configurationProvider.getFeatureFlags()).thenReturn(featureFlags);
      return configurationProvider;
    }
  }
}
