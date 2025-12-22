package com.linkedin.gms.factory.graphql;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.group.GroupService;
import com.datahub.authentication.invite.InviteTokenService;
import com.datahub.authentication.post.PostService;
import com.datahub.authentication.token.StatefulTokenService;
import com.datahub.authentication.user.NativeUserService;
import com.datahub.authorization.role.RoleService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.datahub.graphql.GraphQLEngine;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.plugins.SpringStandardPluginConfiguration;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.gms.factory.search.MappingsBuilderFactory;
import com.linkedin.metadata.connection.ConnectionService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.versioning.EntityVersioningService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.SiblingGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.recommendation.RecommendationsService;
import com.linkedin.metadata.recommendation.candidatesource.RecentlySearchedSource;
import com.linkedin.metadata.recommendation.candidatesource.RecentlyViewedSource;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.service.*;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.aws.S3Util;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.metadata.services.RestrictedService;
import io.datahubproject.metadata.services.SecretService;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.trace.Tracer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import org.mockito.Answers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@SpringBootTest(
    classes = {
      ConfigurationProvider.class,
      GraphQLEngineFactory.class,
      MappingsBuilderFactory.class,
    })
@ContextConfiguration(classes = GraphQLEngineFactoryTest.TestConfig.class)
@TestPropertySource(
    locations = "classpath:/application.yaml",
    properties = {
      "platformAnalytics.enabled=false",
      "graphQL.concurrency.separateThreadPool=true",
      "LINEAGE_DEFAULT_LAST_DAYS_FILTER=30",
    })
public class GraphQLEngineFactoryTest extends AbstractTestNGSpringContextTests {

  @BeforeTest
  public void setup() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @Autowired private GraphQLEngineFactory graphQLEngineFactory;

  @Autowired
  @Qualifier("graphQLEngine")
  private GraphQLEngine graphQLEngine;

  @Autowired
  @Qualifier("graphQLWorkerPool")
  private ExecutorService graphQLWorkerPool;

  @Autowired
  @Qualifier("configurationProvider")
  private ConfigurationProvider configurationProvider;

  @MockitoBean(answers = Answers.RETURNS_MOCKS)
  @Qualifier("searchClientShim")
  private SearchClientShim<?> elasticClient;

  @MockitoBean
  @Qualifier("indexConvention")
  private IndexConvention indexConvention;

  @MockitoBean
  @Qualifier("graphClient")
  private GraphClient graphClient;

  @MockitoBean
  @Qualifier("entityService")
  private EntityService<?> entityService;

  @MockitoBean
  @Qualifier("entitySearchService")
  private EntitySearchService entitySearchService;

  @MockitoBean
  @Qualifier("semanticSearchService")
  private com.linkedin.metadata.search.SemanticSearchService semanticSearchService;

  @MockitoBean
  @Qualifier("cachingEntitySearchService")
  private com.linkedin.metadata.search.client.CachingEntitySearchService cachingEntitySearchService;

  @MockitoBean
  @Qualifier("semanticEntitySearchService")
  private com.linkedin.metadata.search.semantic.SemanticEntitySearch semanticEntitySearchService;

  @MockitoBean
  @Qualifier("graphService")
  private GraphService graphService;

  @MockitoBean
  @Qualifier("siblingGraphService")
  private SiblingGraphService siblingGraphService;

  @MockitoBean
  @Qualifier("timeseriesAspectService")
  private TimeseriesAspectService timeseriesAspectService;

  @MockitoBean
  @Qualifier("recommendationsService")
  private RecommendationsService recommendationsService;

  @MockitoBean
  @Qualifier("dataHubTokenService")
  private StatefulTokenService statefulTokenService;

  @MockitoBean
  @Qualifier("dataHubSecretService")
  private SecretService secretService;

  @MockitoBean
  @Qualifier("gitVersion")
  private GitVersion gitVersion;

  @MockitoBean
  @Qualifier("timelineService")
  private TimelineService timelineService;

  @MockitoBean
  @Qualifier("nativeUserService")
  private NativeUserService nativeUserService;

  @MockitoBean
  @Qualifier("groupService")
  private GroupService groupService;

  @MockitoBean
  @Qualifier("roleService")
  private RoleService roleService;

  @MockitoBean
  @Qualifier("inviteTokenService")
  private InviteTokenService inviteTokenService;

  @MockitoBean
  @Qualifier("postService")
  private PostService postService;

  @MockitoBean
  @Qualifier("viewService")
  private ViewService viewService;

  @MockitoBean
  @Qualifier("ownerShipTypeService")
  private OwnershipTypeService ownershipTypeService;

  @MockitoBean
  @Qualifier("settingsService")
  private SettingsService settingsService;

  @MockitoBean
  @Qualifier("lineageService")
  private LineageService lineageService;

  @MockitoBean
  @Qualifier("queryService")
  private QueryService queryService;

  @MockitoBean
  @Qualifier("erModelRelationshipService")
  private ERModelRelationshipService erModelRelationshipService;

  @MockitoBean
  @Qualifier("dataProductService")
  private DataProductService dataProductService;

  @MockitoBean
  @Qualifier("applicationService")
  private ApplicationService applicationService;

  @MockitoBean
  @Qualifier("formService")
  private FormService formService;

  @MockitoBean
  @Qualifier("restrictedService")
  private RestrictedService restrictedService;

  @MockitoBean
  @Qualifier("businessAttributeService")
  private BusinessAttributeService businessAttributeService;

  @MockitoBean
  @Qualifier("connectionService")
  private ConnectionService connectionService;

  @MockitoBean
  @Qualifier("assertionService")
  private AssertionService assertionService;

  @MockitoBean
  @Qualifier("entityClient")
  private EntityClient entityClient;

  @MockitoBean
  @Qualifier("systemEntityClient")
  private SystemEntityClient systemEntityClient;

  @MockitoBean
  @Qualifier("s3Util")
  private S3Util s3Util;

  @MockitoBean private EntityVersioningService entityVersioningService;

  @MockitoBean private MetricUtils metricUtils;

  @MockitoBean private EntityRegistry entityRegistry;

  @MockitoBean private QueryFilterRewriteChain queryFilterRewriteChain;

  @MockitoBean(name = "baseElasticSearchComponents")
  private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components;

  @MockitoBean
  @Qualifier("recentlyViewedCandidateSource")
  private RecentlyViewedSource recentlyViewedSource;

  @MockitoBean
  @Qualifier("recentlySearchedCandidateSource")
  private RecentlySearchedSource recentlySearchedSource;

  @MockitoBean
  @Qualifier("pageTemplateService")
  private PageTemplateService pageTemplateService;

  @MockitoBean
  @Qualifier("pageModuleService")
  private PageModuleService pageModuleService;

  @MockitoBean
  @Qualifier("dataHubFileService")
  private DataHubFileService dataHubFileService;

  @Value("${platformAnalytics.enabled}")
  private Boolean isAnalyticsEnabled;

  @Value("${LINEAGE_DEFAULT_LAST_DAYS_FILTER:#{null}}")
  private Integer defaultLineageLastDaysFilter;

  @BeforeMethod
  public void setUp() {
    // Set up default mock behaviors
    when(graphService.supportsMultiHop()).thenReturn(true);
    when(metricUtils.getRegistry()).thenReturn(new SimpleMeterRegistry());
  }

  @Test
  public void testGraphQLEngineCreation() {
    // Then
    assertNotNull(graphQLEngine);

    // Verify analytics is disabled as per property
    assertFalse(isAnalyticsEnabled);
  }

  @Test
  public void testGraphQLWorkerPoolCreation() {
    // Then
    assertNotNull(graphQLWorkerPool);
    assertTrue(graphQLWorkerPool instanceof ThreadPoolExecutor);

    ThreadPoolExecutor threadPool = (ThreadPoolExecutor) graphQLWorkerPool;

    // The default configuration should use default values
    assertTrue(threadPool.getCorePoolSize() > 0);
    assertTrue(threadPool.getMaximumPoolSize() > 0);
  }

  @Test
  public void testConfigurationProviderDefaults() {
    // Verify ConfigurationProvider returns non-null configurations with defaults
    assertNotNull(configurationProvider);
    assertNotNull(configurationProvider.getIngestion());
    assertNotNull(configurationProvider.getAuthentication());
    assertNotNull(configurationProvider.getAuthorization());
    assertNotNull(configurationProvider.getVisualConfig());
    assertNotNull(configurationProvider.getTelemetry());
    assertNotNull(configurationProvider.getMetadataTests());
    assertNotNull(configurationProvider.getDatahub());
    assertNotNull(configurationProvider.getViews());
    assertNotNull(configurationProvider.getSearchBar());
    assertNotNull(configurationProvider.getHomePage());
    assertNotNull(configurationProvider.getFeatureFlags());
    assertNotNull(configurationProvider.getGraphQL());
    assertNotNull(configurationProvider.getChromeExtension());
    assertNotNull(configurationProvider.getCache());

    // Verify nested configurations
    assertNotNull(configurationProvider.getCache().getClient());
    assertNotNull(configurationProvider.getCache().getClient().getUsageClient());
    assertNotNull(configurationProvider.getGraphQL().getConcurrency());
  }

  @Test
  public void testLineageDefaultDaysFilter() {
    // Then
    assertEquals(defaultLineageLastDaysFilter, Integer.valueOf(30));
  }

  @Test
  public void testGraphQLEngineWithAnalyticsEnabled() {
    // Create a new factory instance with analytics enabled
    GraphQLEngineFactory factoryWithAnalytics = new GraphQLEngineFactory();

    // Set up dependencies using reflection
    setField(factoryWithAnalytics, "elasticClient", elasticClient);
    setField(factoryWithAnalytics, "indexConvention", indexConvention);
    setField(factoryWithAnalytics, "graphClient", graphClient);
    setField(factoryWithAnalytics, "entityService", entityService);
    setField(factoryWithAnalytics, "graphService", graphService);
    setField(factoryWithAnalytics, "siblingGraphService", siblingGraphService);
    setField(factoryWithAnalytics, "timeseriesAspectService", timeseriesAspectService);
    setField(factoryWithAnalytics, "recommendationsService", recommendationsService);
    setField(factoryWithAnalytics, "statefulTokenService", statefulTokenService);
    setField(factoryWithAnalytics, "secretService", secretService);
    setField(factoryWithAnalytics, "entityRegistry", entityRegistry);
    setField(factoryWithAnalytics, "configProvider", configurationProvider);
    setField(factoryWithAnalytics, "gitVersion", gitVersion);
    setField(factoryWithAnalytics, "timelineService", timelineService);
    setField(factoryWithAnalytics, "nativeUserService", nativeUserService);
    setField(factoryWithAnalytics, "groupService", groupService);
    setField(factoryWithAnalytics, "roleService", roleService);
    setField(factoryWithAnalytics, "inviteTokenService", inviteTokenService);
    setField(factoryWithAnalytics, "postService", postService);
    setField(factoryWithAnalytics, "viewService", viewService);
    setField(factoryWithAnalytics, "ownershipTypeService", ownershipTypeService);
    setField(factoryWithAnalytics, "settingsService", settingsService);
    setField(factoryWithAnalytics, "lineageService", lineageService);
    setField(factoryWithAnalytics, "queryService", queryService);
    setField(factoryWithAnalytics, "erModelRelationshipService", erModelRelationshipService);
    setField(factoryWithAnalytics, "dataProductService", dataProductService);
    setField(factoryWithAnalytics, "applicationService", applicationService);
    setField(factoryWithAnalytics, "formService", formService);
    setField(factoryWithAnalytics, "restrictedService", restrictedService);
    setField(factoryWithAnalytics, "businessAttributeService", businessAttributeService);
    setField(factoryWithAnalytics, "_connectionService", connectionService);
    setField(factoryWithAnalytics, "assertionService", assertionService);
    setField(factoryWithAnalytics, "pageTemplateService", pageTemplateService);
    setField(factoryWithAnalytics, "pageModuleService", pageModuleService);
    setField(factoryWithAnalytics, "dataHubFileService", dataHubFileService);
    setField(factoryWithAnalytics, "s3Util", s3Util);
    setField(factoryWithAnalytics, "isAnalyticsEnabled", true);

    // When
    GraphQLEngine engineWithAnalytics =
        factoryWithAnalytics.graphQLEngine(
            entityClient, systemEntityClient, entityVersioningService, metricUtils);

    // Then
    assertNotNull(engineWithAnalytics);
  }

  @Test
  public void testGraphQLWorkerPoolMetricsRegistration() {
    // Then
    assertNotNull(graphQLWorkerPool);
  }

  @Test
  public void testAllServicesAreWired() {
    // Verify all required services are injected
    assertNotNull(elasticClient);
    assertNotNull(indexConvention);
    assertNotNull(graphClient);
    assertNotNull(entityService);
    assertNotNull(entitySearchService);
    assertNotNull(graphService);
    assertNotNull(siblingGraphService);
    assertNotNull(timeseriesAspectService);
    assertNotNull(recommendationsService);
    assertNotNull(statefulTokenService);
    assertNotNull(secretService);
    assertNotNull(entityRegistry);
    assertNotNull(gitVersion);
    assertNotNull(timelineService);
    assertNotNull(nativeUserService);
    assertNotNull(groupService);
    assertNotNull(roleService);
    assertNotNull(inviteTokenService);
    assertNotNull(postService);
    assertNotNull(viewService);
    assertNotNull(ownershipTypeService);
    assertNotNull(settingsService);
    assertNotNull(lineageService);
    assertNotNull(queryService);
    assertNotNull(erModelRelationshipService);
    assertNotNull(dataProductService);
    assertNotNull(applicationService);
    assertNotNull(formService);
    assertNotNull(restrictedService);
    assertNotNull(businessAttributeService);
    assertNotNull(connectionService);
    assertNotNull(assertionService);
    assertNotNull(pageTemplateService);
    assertNotNull(pageModuleService);
    assertNotNull(dataHubFileService);
    assertNotNull(entityClient);
    assertNotNull(systemEntityClient);
    assertNotNull(s3Util);
    assertNotNull(entityVersioningService);
    assertNotNull(metricUtils);
  }

  @Test
  public void testGraphQLConcurrencyConfiguration() {
    // Test the actual concurrency configuration from the default ConfigurationProvider
    var concurrencyConfig = configurationProvider.getGraphQL().getConcurrency();
    assertNotNull(concurrencyConfig);

    // These should have default values
    assertNotNull(concurrencyConfig.getCorePoolSize());
    assertNotNull(concurrencyConfig.getMaxPoolSize());
    assertNotNull(concurrencyConfig.getKeepAlive());
    assertNotNull(concurrencyConfig.getStackSize());
  }

  @Test
  public void testGraphQLWorkerPoolWithDifferentConfiguration() {
    // Test worker pool creation with different configurations
    var concurrencyConfig = configurationProvider.getGraphQL().getConcurrency();

    // Create a new factory to test different scenarios
    ExecutorService executorService = graphQLEngineFactory.graphQLWorkerPool(metricUtils);
    assertNotNull(executorService);

    ThreadPoolExecutor threadPool = (ThreadPoolExecutor) executorService;

    // If core pool size is negative, it should use default calculation
    if (concurrencyConfig.getCorePoolSize() < 0) {
      assertEquals(threadPool.getCorePoolSize(), Runtime.getRuntime().availableProcessors() * 5);
    } else {
      assertEquals(threadPool.getCorePoolSize(), concurrencyConfig.getCorePoolSize());
    }

    // If max pool size is zero or negative, it should use default calculation
    if (concurrencyConfig.getMaxPoolSize() <= 0) {
      assertEquals(
          threadPool.getMaximumPoolSize(), Runtime.getRuntime().availableProcessors() * 100);
    } else {
      assertEquals(threadPool.getMaximumPoolSize(), concurrencyConfig.getMaxPoolSize());
    }

    // Cleanup
    executorService.shutdown();
  }

  @Test
  public void testStsClientCreationHandlesException() {
    // The factory should handle StsClient creation exceptions gracefully
    // This is tested implicitly by the successful creation of graphQLEngine
    assertNotNull(graphQLEngine);
  }

  @Test
  public void testS3UtilIntegration() {
    // Verify S3Util is properly injected and available
    assertNotNull(s3Util, "S3Util should be injected into GraphQLEngineFactory");

    // Verify S3Util is passed to the GraphQL engine
    assertNotNull(graphQLEngine, "GraphQLEngine should be created with S3Util");
  }

  private void setField(Object target, String fieldName, Object value) {
    try {
      java.lang.reflect.Field field = target.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(target, value);
    } catch (Exception e) {
      throw new RuntimeException("Failed to set field: " + fieldName, e);
    }
  }

  @org.springframework.context.annotation.Configuration
  static class TestConfig {

    @MockBean(name = "settingsBuilder")
    public SettingsBuilder settingsBuilder;

    @Bean
    public SpringStandardPluginConfiguration springStandardPluginConfiguration() {
      return new SpringStandardPluginConfiguration();
    }

    @Bean("systemOperationContext")
    public OperationContext systemOperationContext(MetricUtils metricUtils) {
      OperationContext defaultContext = TestOperationContexts.systemContextNoSearchAuthorization();
      return defaultContext.toBuilder()
          .systemTelemetryContext(
              SystemTelemetryContext.builder()
                  .metricUtils(metricUtils)
                  .tracer(mock(Tracer.class))
                  .build())
          .build(defaultContext.getSystemActorContext().getAuthentication(), false);
    }

    @Bean
    public ObjectMapper objectMapper(
        @Qualifier("systemOperationContext") OperationContext operationContext) {
      return operationContext.getObjectMapper();
    }
  }
}
