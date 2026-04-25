package io.datahubproject.openapi.v3.controller;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entity.versioning.EntityVersioningServiceFactory;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.UpdateAspectResult;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.openapi.config.GlobalControllerExceptionHandler;
import io.datahubproject.openapi.config.SpringWebConfig;
import io.datahubproject.openapi.config.TracingInterceptor;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for createAspect and patchAspect operations with domain-based authorization.
 *
 * <p>These tests focus on single-aspect operations (POST/PATCH to specific aspects) which use
 * domain-based authorization differently than batch operations.
 */
@SpringBootTest(classes = {SpringWebConfig.class})
@ComponentScan(basePackages = {"io.datahubproject.openapi.v3.controller.EntityController"})
@Import({
  SpringWebConfig.class,
  TracingInterceptor.class,
  EntityController.class,
  GenericEntitiesControllerAspectOperationsTest.TestConfig.class,
  EntityVersioningServiceFactory.class,
  GlobalControllerExceptionHandler.class
})
@AutoConfigureWebMvc
@AutoConfigureMockMvc
public class GenericEntitiesControllerAspectOperationsTest
    extends AbstractTestNGSpringContextTests {

  @Autowired private MockMvc mockMvc;
  @Autowired private EntityService<?> mockEntityService;
  @Autowired private AuthorizerChain mockAuthorizerChain;
  @Autowired private ConfigurationProvider mockConfigurationProvider;

  private FeatureFlags featureFlags;

  private static final String DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:test,table,PROD)";
  private static final String FINANCE_DOMAIN_URN = "urn:li:domain:finance";

  @BeforeMethod
  public void setup() {
    reset(mockEntityService, mockAuthorizerChain, mockConfigurationProvider);

    Authentication authentication = mock(Authentication.class);
    when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "datahub"));
    AuthenticationContext.setAuthentication(authentication);

    featureFlags = new FeatureFlags();
    featureFlags.setDomainBasedAuthorizationEnabled(true);
    when(mockConfigurationProvider.getFeatureFlags()).thenReturn(featureFlags);

    when(mockAuthorizerChain.authorize(any()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
  }

  /**
   * Test createAspect operation with domain-based authorization enabled.
   *
   * <p>When domain auth is enabled, createAspect bypasses standard entity-level checks and relies
   * on domain validation inside the transaction.
   */
  @Test
  public void testCreateAspect_WithDomainAuthEnabled_Authorized() throws Exception {
    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);
    Urn domainUrn = UrnUtils.getUrn(FINANCE_DOMAIN_URN);

    // Mock the existing entity to check authorization
    Domains existingDomains = new Domains();
    existingDomains.setDomains(new UrnArray(Collections.singletonList(domainUrn)));

    EntityResponse mockEntityResponse = new EntityResponse();
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(existingDomains.data()));
    aspectMap.put("domains", envelopedAspect);
    mockEntityResponse.setAspects(aspectMap);

    when(mockEntityService.getEntityV2(any(), eq("dataset"), eq(datasetUrn), any()))
        .thenReturn(mockEntityResponse);

    // Mock BatchItem with proper RecordTemplate
    Domains domains = new Domains();
    domains.setDomains(new UrnArray(Collections.singletonList(domainUrn)));
    BatchItem mockBatchItem = mock(BatchItem.class);
    when(mockBatchItem.getAspectName()).thenReturn("domains");
    when(mockBatchItem.getRecordTemplate()).thenReturn(domains);

    // Mock UpdateAspectResult
    UpdateAspectResult mockResult = mock(UpdateAspectResult.class);
    when(mockResult.getUrn()).thenReturn(datasetUrn);
    when(mockResult.getNewValue()).thenReturn(domains);

    when(mockEntityService.ingestProposal(any(OperationContext.class), any(), anyBoolean()))
        .thenReturn(
            Collections.singletonList(
                IngestResult.builder()
                    .urn(datasetUrn)
                    .request(mockBatchItem)
                    .result(mockResult)
                    .sqlCommitted(true)
                    .build()));

    // Wrap JSON in "value" field as expected by toUpsertItem
    String requestBody = "{\"value\": {\"domains\": [\"" + FINANCE_DOMAIN_URN + "\"]}}";

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/entity/dataset/" + DATASET_URN + "/domains")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    verify(mockEntityService).ingestProposal(any(OperationContext.class), any(), anyBoolean());
  }

  /**
   * Test createAspect with domain auth disabled falls back to standard authorization.
   *
   * <p>When domain auth is disabled, standard entity-level authorization should be enforced.
   */
  @Test
  public void testCreateAspect_WithDomainAuthDisabled_UsesStandardAuth() throws Exception {
    // Disable domain-based auth
    featureFlags.setDomainBasedAuthorizationEnabled(false);
    when(mockConfigurationProvider.getFeatureFlags()).thenReturn(featureFlags);

    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);

    // Mock BatchItem with proper RecordTemplate
    Status status = new Status();
    status.setRemoved(false);
    BatchItem mockBatchItem = mock(BatchItem.class);
    when(mockBatchItem.getAspectName()).thenReturn("status");
    when(mockBatchItem.getRecordTemplate()).thenReturn(status);

    // Mock UpdateAspectResult
    UpdateAspectResult mockResult = mock(UpdateAspectResult.class);
    when(mockResult.getUrn()).thenReturn(datasetUrn);
    when(mockResult.getNewValue()).thenReturn(status);

    when(mockEntityService.ingestProposal(any(OperationContext.class), any(), anyBoolean()))
        .thenReturn(
            Collections.singletonList(
                IngestResult.builder()
                    .urn(datasetUrn)
                    .request(mockBatchItem)
                    .result(mockResult)
                    .sqlCommitted(true)
                    .build()));

    // Wrap JSON in "value" field as expected by toUpsertItem
    String requestBody = "{\"value\": {\"removed\": false}}";

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/entity/dataset/" + DATASET_URN + "/status")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    verify(mockEntityService).ingestProposal(any(OperationContext.class), any(), anyBoolean());
  }

  /** Test createAspect when feature flags are null (domain auth effectively disabled). */
  @Test
  public void testCreateAspect_NullFeatureFlags_UsesStandardAuth() throws Exception {
    // Null feature flags
    when(mockConfigurationProvider.getFeatureFlags()).thenReturn(null);

    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);

    // Mock BatchItem with proper RecordTemplate
    Status status = new Status();
    status.setRemoved(false);
    BatchItem mockBatchItem = mock(BatchItem.class);
    when(mockBatchItem.getAspectName()).thenReturn("status");
    when(mockBatchItem.getRecordTemplate()).thenReturn(status);

    // Mock UpdateAspectResult
    UpdateAspectResult mockResult = mock(UpdateAspectResult.class);
    when(mockResult.getUrn()).thenReturn(datasetUrn);
    when(mockResult.getNewValue()).thenReturn(status);

    when(mockEntityService.ingestProposal(any(OperationContext.class), any(), anyBoolean()))
        .thenReturn(
            Collections.singletonList(
                IngestResult.builder()
                    .urn(datasetUrn)
                    .request(mockBatchItem)
                    .result(mockResult)
                    .sqlCommitted(true)
                    .build()));

    // Wrap JSON in "value" field as expected by toUpsertItem
    String requestBody = "{\"value\": {\"removed\": false}}";

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/entity/dataset/" + DATASET_URN + "/status")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    verify(mockEntityService).ingestProposal(any(OperationContext.class), any(), anyBoolean());
  }

  /**
   * Test patchAspect operation with domain-based authorization enabled.
   *
   * <p>Similar to createAspect, patchAspect bypasses standard checks when domain auth is enabled.
   */
  @Test
  public void testPatchAspect_WithDomainAuthEnabled_Authorized() throws Exception {
    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);
    Urn domainUrn = UrnUtils.getUrn(FINANCE_DOMAIN_URN);

    // Mock BatchItem with proper RecordTemplate
    Domains domains = new Domains();
    domains.setDomains(new UrnArray(Collections.singletonList(domainUrn)));
    BatchItem mockBatchItem = mock(BatchItem.class);
    when(mockBatchItem.getRecordTemplate()).thenReturn(domains);

    when(mockEntityService.ingestProposal(any(OperationContext.class), any(), any(), anyBoolean()))
        .thenReturn(
            IngestResult.builder()
                .urn(datasetUrn)
                .request(mockBatchItem)
                .sqlCommitted(true)
                .build());

    String patchBody =
        "{"
            + "\"patch\": ["
            + "  {\"op\": \"add\", \"path\": \"/domains/-\", \"value\": \""
            + FINANCE_DOMAIN_URN
            + "\"}"
            + "]"
            + "}";

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch("/openapi/v3/entity/dataset/" + DATASET_URN + "/domains")
                .content(patchBody)
                .contentType("application/json-patch+json"))
        .andExpect(status().is2xxSuccessful());

    verify(mockEntityService)
        .ingestProposal(any(OperationContext.class), any(), any(), anyBoolean());
  }

  /** Test patchAspect with domain auth disabled uses standard authorization. */
  @Test
  public void testPatchAspect_WithDomainAuthDisabled_UsesStandardAuth() throws Exception {
    // Disable domain-based auth
    featureFlags.setDomainBasedAuthorizationEnabled(false);
    when(mockConfigurationProvider.getFeatureFlags()).thenReturn(featureFlags);

    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);

    // Mock BatchItem with proper RecordTemplate
    Status status = new Status();
    status.setRemoved(false);
    BatchItem mockBatchItem = mock(BatchItem.class);
    when(mockBatchItem.getRecordTemplate()).thenReturn(status);

    when(mockEntityService.ingestProposal(any(OperationContext.class), any(), any(), anyBoolean()))
        .thenReturn(
            IngestResult.builder()
                .urn(datasetUrn)
                .request(mockBatchItem)
                .sqlCommitted(true)
                .build());

    String patchBody =
        "{"
            + "\"patch\": ["
            + "  {\"op\": \"replace\", \"path\": \"/removed\", \"value\": false}"
            + "]"
            + "}";

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch("/openapi/v3/entity/dataset/" + DATASET_URN + "/status")
                .content(patchBody)
                .contentType("application/json-patch+json"))
        .andExpect(status().is2xxSuccessful());

    verify(mockEntityService)
        .ingestProposal(any(OperationContext.class), any(), any(), anyBoolean());
  }

  /**
   * Test patchAspect with domains aspect and domain auth enabled.
   *
   * <p>This tests the path where a PATCH operation modifies the domains aspect itself.
   */
  @Test
  public void testPatchAspect_DomainsAspect_WithDomainAuthEnabled() throws Exception {
    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);
    Urn domainUrn = UrnUtils.getUrn(FINANCE_DOMAIN_URN);

    // Mock existing entity with domains
    Domains existingDomains = new Domains();
    existingDomains.setDomains(new UrnArray(Collections.singletonList(domainUrn)));

    EntityResponse mockEntityResponse = new EntityResponse();
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(existingDomains.data()));
    aspectMap.put("domains", envelopedAspect);
    mockEntityResponse.setAspects(aspectMap);

    when(mockEntityService.getEntityV2(any(), eq("dataset"), eq(datasetUrn), any()))
        .thenReturn(mockEntityResponse);

    // Mock BatchItem with proper RecordTemplate
    Domains domains = new Domains();
    domains.setDomains(new UrnArray(Collections.singletonList(domainUrn)));
    BatchItem mockBatchItem = mock(BatchItem.class);
    when(mockBatchItem.getRecordTemplate()).thenReturn(domains);

    when(mockEntityService.ingestProposal(any(OperationContext.class), any(), any(), anyBoolean()))
        .thenReturn(
            IngestResult.builder()
                .urn(datasetUrn)
                .request(mockBatchItem)
                .sqlCommitted(true)
                .build());

    String patchBody =
        "{"
            + "\"patch\": ["
            + "  {\"op\": \"replace\", \"path\": \"/domains\", \"value\": [\""
            + FINANCE_DOMAIN_URN
            + "\"]}"
            + "]"
            + "}";

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch("/openapi/v3/entity/dataset/" + DATASET_URN + "/domains")
                .content(patchBody)
                .contentType("application/json-patch+json"))
        .andExpect(status().is2xxSuccessful());

    verify(mockEntityService)
        .ingestProposal(any(OperationContext.class), any(), any(), anyBoolean());
  }

  /** Test createAspect when entity doesn't exist and createIfEntityNotExists is false. */
  @Test
  public void testCreateAspect_EntityNotExists_NotFound() throws Exception {
    // Return empty list to trigger 404 via Optional.empty()
    when(mockEntityService.ingestProposal(any(OperationContext.class), any(), anyBoolean()))
        .thenReturn(Collections.emptyList());

    // Wrap JSON in "value" field as expected by toUpsertItem
    String requestBody = "{\"value\": {\"removed\": false}}";

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/entity/dataset/" + DATASET_URN + "/status")
                .param("createIfEntityNotExists", "false")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  /** Test patchAspect when entity doesn't exist returns 404. */
  @Test
  public void testPatchAspect_EntityNotExists_NotFound() throws Exception {
    when(mockEntityService.ingestProposal(any(OperationContext.class), any(), any(), anyBoolean()))
        .thenReturn(null);

    String patchBody =
        "{"
            + "\"patch\": ["
            + "  {\"op\": \"replace\", \"path\": \"/removed\", \"value\": false}"
            + "]"
            + "}";

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch("/openapi/v3/entity/dataset/" + DATASET_URN + "/status")
                .content(patchBody)
                .contentType("application/json-patch+json"))
        .andExpect(status().isNotFound());
  }

  @TestConfiguration
  public static class TestConfig {
    @MockBean public EntityServiceImpl entityService;
    @MockBean public SearchService searchService;
    @MockBean public TimeseriesAspectService timeseriesAspectService;
    @MockBean public SystemTelemetryContext systemTelemetryContext;
    @MockBean public ConfigurationProvider configurationProvider;

    @Bean
    public AuthorizerChain authorizerChain() {
      return mock(AuthorizerChain.class);
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
  }
}
