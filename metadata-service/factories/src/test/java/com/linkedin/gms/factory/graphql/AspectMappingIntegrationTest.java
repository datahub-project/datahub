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
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.datahub.graphql.GraphQLEngine;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.plugins.SpringStandardPluginConfiguration;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.gms.factory.search.MappingsBuilderFactory;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.connection.ConnectionService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.versioning.EntityVersioningService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.SiblingGraphService;
import com.linkedin.metadata.key.DatasetKey;
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
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.metadata.services.RestrictedService;
import io.datahubproject.metadata.services.SecretService;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.trace.Tracer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Integration test for AspectMappingRegistry and aspect optimization in GraphQL queries.
 *
 * <p>This test validates the end-to-end flow: 1. GraphQL query with specific field selections 2.
 * AspectMappingRegistry determines required aspects 3. EntityClient is called with only the
 * necessary aspects
 */
@SpringBootTest(
    classes = {
      ConfigurationProvider.class,
      GraphQLEngineFactory.class,
      MappingsBuilderFactory.class
    })
@ContextConfiguration(classes = AspectMappingIntegrationTest.TestConfig.class)
@TestPropertySource(
    locations = "classpath:/application.yaml",
    properties = {"platformAnalytics.enabled=false"})
public class AspectMappingIntegrationTest extends AbstractTestNGSpringContextTests {

  @BeforeTest
  public void setup() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @Autowired
  @Qualifier("graphQLEngine")
  private GraphQLEngine graphQLEngine;

  @MockitoBean
  @Qualifier("entityClient")
  private EntityClient entityClient;

  @MockitoBean
  @Qualifier("systemEntityClient")
  private SystemEntityClient systemEntityClient;

  @MockitoBean
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

  private static final String TEST_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,test.db,PROD)";

  @BeforeMethod
  public void setUp() {
    Mockito.reset(entityClient);
    when(graphService.supportsMultiHop()).thenReturn(true);
    when(metricUtils.getRegistry()).thenReturn(new SimpleMeterRegistry());
  }

  @Test
  public void testGraphQLEngineHasAspectMappingRegistry() {
    assertNotNull(graphQLEngine);
    assertNotNull(graphQLEngine.getGraphQL());
    assertNotNull(graphQLEngine.getGraphQL().getGraphQLSchema());
  }

  @Test
  public void testQueryWithMinimalFieldsUsesOptimizedAspects() throws Exception {
    Urn datasetUrn = Urn.createFromString(TEST_DATASET_URN);
    DatasetKey datasetKey =
        new DatasetKey()
            .setPlatform(Urn.createFromTuple("dataPlatform", "mysql"))
            .setName("test.db")
            .setOrigin(com.linkedin.common.FabricType.PROD);
    DatasetProperties datasetProperties =
        new DatasetProperties().setDescription("test description").setName("Test Dataset");

    when(entityClient.batchGetV2(any(), any(), any(), any()))
        .thenReturn(
            ImmutableMap.of(
                datasetUrn,
                new EntityResponse()
                    .setEntityName(Constants.DATASET_ENTITY_NAME)
                    .setUrn(datasetUrn)
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.DATASET_KEY_ASPECT_NAME,
                                new EnvelopedAspect().setValue(new Aspect(datasetKey.data())),
                                Constants.DATASET_PROPERTIES_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(datasetProperties.data())))))));

    String minimalQuery =
        String.format("{ dataset(urn: \"%s\") { urn type name } }", TEST_DATASET_URN);

    ExecutionInput executionInput =
        ExecutionInput.newExecutionInput()
            .query(minimalQuery)
            .context(
                TestOperationContexts.systemContextNoSearchAuthorization()
                    .withAsyncContext(() -> graphQLEngine.getContext(null, null, null)))
            .build();

    ExecutionResult result = graphQLEngine.getGraphQL().execute(executionInput);

    assertNotNull(result);
    assertTrue(result.getErrors().isEmpty(), "Query should execute without errors");

    ArgumentCaptor<Set<String>> aspectsCaptor = ArgumentCaptor.forClass(Set.class);
    verify(entityClient, atLeastOnce())
        .batchGetV2(
            any(), eq(Constants.DATASET_ENTITY_NAME), any(HashSet.class), aspectsCaptor.capture());

    Set<String> capturedAspects = aspectsCaptor.getValue();

    assertTrue(
        capturedAspects.size() < 20,
        "Optimized query should fetch fewer aspects. Got: " + capturedAspects.size());
    assertTrue(
        capturedAspects.contains("datasetKey"),
        "Should include datasetKey. Got: " + capturedAspects);
  }

  @Test
  public void testAspectMappingRegistryBuiltFromSchema() {
    assertNotNull(graphQLEngine);

    Map<String, Object> schemaDefinition =
        graphQLEngine.getGraphQL().getGraphQLSchema().getTypeMap();
    assertNotNull(schemaDefinition);

    assertTrue(schemaDefinition.containsKey("Dataset"), "Schema should contain Dataset type");
  }

  @org.springframework.context.annotation.Configuration
  static class TestConfig {

    @MockitoBean(name = "settingsBuilder")
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
