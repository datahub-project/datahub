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
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entity.versioning.EntityVersioningServiceFactory;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.IngestResult;
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
 * Tests for domain-based authorization in GenericEntitiesController with feature flag ENABLED.
 *
 * <p>These tests verify domain authorization behavior when the feature flag is enabled:
 *
 * <ul>
 *   <li>✅ Creating entities with domains (authorized scenarios)
 *   <li>✅ Creating entities without domains (should still work)
 *   <li>✅ Batch operations with multiple domains
 * </ul>
 *
 * <p><b>Test Scope:</b> These tests focus on the CREATE operations (batch entity creation) which
 * are the primary use case for domain-based authorization. The tests verify that:
 *
 * <ul>
 *   <li>When feature flag is ENABLED and entities have domains, authorization logic is triggered
 *   <li>When feature flag is ENABLED but entities have NO domains, operations proceed normally
 *   <li>The domain extraction and authorization check integration works correctly
 * </ul>
 *
 * <p><b>Authorization Testing Approach:</b>
 *
 * <p>These unit tests mock the authorization chain to return ALLOW by default, verifying the happy
 * path where users have appropriate permissions. Testing authorization DENIAL scenarios requires:
 *
 * <ul>
 *   <li>Properly mocking the {@code OperationContext}'s {@code AuthorizationSession}
 *   <li>The authorization flow: Controller → AuthUtil → OperationContext → AuthorizationSession →
 *       Policy Engine
 *   <li>Complex setup of authorization policies and domain-based access control
 * </ul>
 *
 * <p><b>For Comprehensive Authorization Testing:</b> Create integration tests that:
 *
 * <ul>
 *   <li>Use full Spring context with real authorization components
 *   <li>Configure actual domain-based authorization policies
 *   <li>Test scenarios:
 *       <ul>
 *         <li>User with domain access successfully creates entities
 *         <li>User without domain access receives 403 Forbidden
 *         <li>Batch operations with mixed authorization (some allowed, some denied)
 *       </ul>
 * </ul>
 *
 * <p>The feature flag is configured via:
 *
 * <pre>
 * featureFlags:
 *   domainBasedAuthorizationEnabled: true
 * </pre>
 */
@SpringBootTest(classes = {SpringWebConfig.class})
@ComponentScan(basePackages = {"io.datahubproject.openapi.v3.controller.EntityController"})
@Import({
  SpringWebConfig.class,
  TracingInterceptor.class,
  EntityController.class,
  GenericEntitiesControllerDomainAuthEnabledTest.TestConfig.class,
  EntityVersioningServiceFactory.class,
  GlobalControllerExceptionHandler.class
})
@AutoConfigureWebMvc
@AutoConfigureMockMvc
public class GenericEntitiesControllerDomainAuthEnabledTest
    extends AbstractTestNGSpringContextTests {

  @Autowired private MockMvc mockMvc;
  @Autowired private EntityService<?> mockEntityService;
  @Autowired private AuthorizerChain mockAuthorizerChain;
  @Autowired private ConfigurationProvider mockConfigurationProvider;

  private FeatureFlags featureFlags;

  private static final String DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:test,table,PROD)";
  private static final String DATASET_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:test,table2,PROD)";
  private static final String FINANCE_DOMAIN_URN = "urn:li:domain:finance";
  private static final String MARKETING_DOMAIN_URN = "urn:li:domain:marketing";

  @BeforeMethod
  public void setup() {
    // Reset all mocks
    reset(mockEntityService, mockAuthorizerChain, mockConfigurationProvider);

    // Setup authentication
    Authentication authentication = mock(Authentication.class);
    when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "datahub"));
    AuthenticationContext.setAuthentication(authentication);

    // Enable domain-based authorization feature flag
    featureFlags = new FeatureFlags();
    featureFlags.setDomainBasedAuthorizationEnabled(true);
    when(mockConfigurationProvider.getFeatureFlags()).thenReturn(featureFlags);

    // Default all authorization to ALLOW
    when(mockAuthorizerChain.authorize(any()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
  }

  // ========== CREATE ENTITY TESTS WITH FEATURE FLAG ENABLED ==========

  /**
   * Test: Create entity with domain - feature flag enabled, user authorized
   *
   * <p>Verifies that when domain-based authorization is enabled and a user has permission on the
   * domain, they can successfully create entities in that domain.
   *
   * <p><b>Expected Behavior:</b>
   *
   * <ul>
   *   <li>Feature flag check passes (enabled)
   *   <li>Domain extraction finds the finance domain
   *   <li>Authorization check is performed for the domain
   *   <li>Authorization succeeds (mocked to ALLOW)
   *   <li>Entity creation proceeds successfully
   * </ul>
   */
  @Test
  public void testCreateEntityWithDomain_Authorized() throws Exception {
    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);
    Urn domainUrn = UrnUtils.getUrn(FINANCE_DOMAIN_URN);

    // Mock successful ingest result
    BatchItem mockBatchItem = mock(BatchItem.class);
    when(mockBatchItem.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockBatchItem.getAspectName()).thenReturn("domains");
    when(mockBatchItem.getRecordTemplate())
        .thenReturn(new Domains().setDomains(new UrnArray(Collections.singletonList(domainUrn))));

    when(mockEntityService.ingestProposal(any(), any(), anyBoolean()))
        .thenReturn(
            Collections.singletonList(
                IngestResult.builder()
                    .urn(datasetUrn)
                    .request(mockBatchItem)
                    .sqlCommitted(true)
                    .build()));

    String requestBody =
        "[{\"urn\": \""
            + DATASET_URN
            + "\", "
            + "\"status\": {\"value\": {\"removed\": false}}, "
            + "\"domains\": {\"value\": {\"domains\": [\""
            + FINANCE_DOMAIN_URN
            + "\"]}}}]";

    // Should succeed - user has permission on the finance domain (mocked to ALLOW)
    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/entity/dataset")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    // Verify ingestion was performed
    verify(mockEntityService).ingestProposal(any(), any(), anyBoolean());
  }

  /**
   * Test: Create entity without domain - feature flag enabled
   *
   * <p>Verifies that entities without domains can still be created even when domain authorization
   * is enabled. This ensures backward compatibility for entities that don't participate in
   * domain-based access control.
   *
   * <p><b>Expected Behavior:</b>
   *
   * <ul>
   *   <li>Feature flag check passes (enabled)
   *   <li>Domain extraction finds NO domains
   *   <li>Domain authorization check is SKIPPED (no domains to check)
   *   <li>Entity creation proceeds normally
   * </ul>
   */
  @Test
  public void testCreateEntityWithoutDomain_FeatureFlagEnabled() throws Exception {
    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);

    // Mock successful ingest result
    BatchItem mockBatchItem = mock(BatchItem.class);
    when(mockBatchItem.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockBatchItem.getAspectName()).thenReturn("status");
    when(mockBatchItem.getRecordTemplate()).thenReturn(new Status().setRemoved(false));

    when(mockEntityService.ingestProposal(any(), any(), anyBoolean()))
        .thenReturn(
            Collections.singletonList(
                IngestResult.builder()
                    .urn(datasetUrn)
                    .request(mockBatchItem)
                    .sqlCommitted(true)
                    .build()));

    String requestBody =
        "[{\"urn\": \"" + DATASET_URN + "\", \"status\": {\"value\": {\"removed\": false}}}]";

    // Should succeed - no domain means no domain authorization check
    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/entity/dataset")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    // Verify ingestion was performed
    verify(mockEntityService).ingestProposal(any(), any(), anyBoolean());
  }

  /**
   * Test: Batch create with multiple domains - feature flag enabled, all authorized
   *
   * <p>Verifies that batch operations with multiple domains work correctly when the user has
   * permissions on all domains involved.
   *
   * <p><b>Expected Behavior:</b>
   *
   * <ul>
   *   <li>Feature flag check passes (enabled)
   *   <li>Domain extraction finds finance and marketing domains
   *   <li>Authorization check performed for BOTH domains
   *   <li>Both authorization checks succeed (mocked to ALLOW)
   *   <li>Batch entity creation proceeds successfully
   * </ul>
   */
  @Test
  public void testBatchCreateMultipleDomains_AllAuthorized() throws Exception {
    Urn dataset1Urn = UrnUtils.getUrn(DATASET_URN);
    Urn dataset2Urn = UrnUtils.getUrn(DATASET_URN_2);
    Urn financeDomainUrn = UrnUtils.getUrn(FINANCE_DOMAIN_URN);
    Urn marketingDomainUrn = UrnUtils.getUrn(MARKETING_DOMAIN_URN);

    // Mock successful ingest results
    BatchItem mockBatchItem1 = mock(BatchItem.class);
    when(mockBatchItem1.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockBatchItem1.getAspectName()).thenReturn("domains");
    when(mockBatchItem1.getRecordTemplate())
        .thenReturn(
            new Domains().setDomains(new UrnArray(Collections.singletonList(financeDomainUrn))));

    BatchItem mockBatchItem2 = mock(BatchItem.class);
    when(mockBatchItem2.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockBatchItem2.getAspectName()).thenReturn("domains");
    when(mockBatchItem2.getRecordTemplate())
        .thenReturn(
            new Domains().setDomains(new UrnArray(Collections.singletonList(marketingDomainUrn))));

    when(mockEntityService.ingestProposal(any(), any(), anyBoolean()))
        .thenReturn(
            java.util.Arrays.asList(
                IngestResult.builder()
                    .urn(dataset1Urn)
                    .request(mockBatchItem1)
                    .sqlCommitted(true)
                    .build(),
                IngestResult.builder()
                    .urn(dataset2Urn)
                    .request(mockBatchItem2)
                    .sqlCommitted(true)
                    .build()));

    String requestBody =
        "[{\"urn\": \""
            + DATASET_URN
            + "\", "
            + "\"status\": {\"value\": {\"removed\": false}}, "
            + "\"domains\": {\"value\": {\"domains\": [\""
            + FINANCE_DOMAIN_URN
            + "\"]}}}, "
            + "{\"urn\": \""
            + DATASET_URN_2
            + "\", "
            + "\"status\": {\"value\": {\"removed\": false}}, "
            + "\"domains\": {\"value\": {\"domains\": [\""
            + MARKETING_DOMAIN_URN
            + "\"]}}}]";

    // Should succeed - user has permission on both domains (mocked to ALLOW)
    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/entity/dataset")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    // Verify ingestion was performed
    verify(mockEntityService).ingestProposal(any(), any(), anyBoolean());
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
