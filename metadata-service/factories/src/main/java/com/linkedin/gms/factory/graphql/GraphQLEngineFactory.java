package com.linkedin.gms.factory.graphql;

import com.datahub.authentication.group.GroupService;
import com.datahub.authentication.invite.InviteTokenService;
import com.datahub.authentication.post.PostService;
import com.datahub.authentication.token.StatefulTokenService;
import com.datahub.authentication.user.NativeUserService;
import com.datahub.authorization.role.RoleService;
import com.linkedin.datahub.graphql.GmsGraphQLEngine;
import com.linkedin.datahub.graphql.GmsGraphQLEngineArgs;
import com.linkedin.datahub.graphql.GraphQLEngine;
import com.linkedin.datahub.graphql.analytics.service.AnalyticsService;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.auth.DataHubTokenServiceFactory;
import com.linkedin.gms.factory.common.GitVersionFactory;
import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.common.RestHighLevelClientFactory;
import com.linkedin.gms.factory.common.SiblingGraphServiceFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.recommendation.RecommendationServiceFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.SiblingGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.recommendation.RecommendationsService;
import com.linkedin.metadata.service.DataProductService;
import com.linkedin.metadata.service.ERModelRelationshipService;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.service.LineageService;
import com.linkedin.metadata.service.OwnershipTypeService;
import com.linkedin.metadata.service.QueryService;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.usage.RestliUsageClient;
import io.datahubproject.metadata.services.RestrictedService;
import io.datahubproject.metadata.services.SecretService;
import javax.annotation.Nonnull;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({
  RestHighLevelClientFactory.class,
  IndexConventionFactory.class,
  RecommendationServiceFactory.class,
  EntityRegistryFactory.class,
  DataHubTokenServiceFactory.class,
  GitVersionFactory.class,
  SiblingGraphServiceFactory.class
})
public class GraphQLEngineFactory {
  @Autowired
  @Qualifier("elasticSearchRestHighLevelClient")
  private RestHighLevelClient elasticClient;

  @Autowired
  @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN)
  private IndexConvention indexConvention;

  @Autowired
  @Qualifier("graphClient")
  private GraphClient graphClient;

  @Autowired
  @Qualifier("usageClient")
  private RestliUsageClient usageClient;

  @Autowired
  @Qualifier("entityService")
  private EntityService<?> entityService;

  @Autowired
  @Qualifier("graphService")
  private GraphService graphService;

  @Autowired
  @Qualifier("siblingGraphService")
  private SiblingGraphService siblingGraphService;

  @Autowired
  @Qualifier("timeseriesAspectService")
  private TimeseriesAspectService timeseriesAspectService;

  @Autowired
  @Qualifier("recommendationsService")
  private RecommendationsService recommendationsService;

  @Autowired
  @Qualifier("dataHubTokenService")
  private StatefulTokenService statefulTokenService;

  @Autowired
  @Qualifier("dataHubSecretService")
  private SecretService secretService;

  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry entityRegistry;

  @Autowired
  @Qualifier("configurationProvider")
  private ConfigurationProvider configProvider;

  @Autowired
  @Qualifier("gitVersion")
  private GitVersion gitVersion;

  @Autowired
  @Qualifier("timelineService")
  private TimelineService timelineService;

  @Autowired
  @Qualifier("nativeUserService")
  private NativeUserService nativeUserService;

  @Autowired
  @Qualifier("groupService")
  private GroupService groupService;

  @Autowired
  @Qualifier("roleService")
  private RoleService roleService;

  @Autowired
  @Qualifier("inviteTokenService")
  private InviteTokenService inviteTokenService;

  @Autowired
  @Qualifier("postService")
  private PostService postService;

  @Autowired
  @Qualifier("viewService")
  private ViewService viewService;

  @Autowired
  @Qualifier("ownerShipTypeService")
  private OwnershipTypeService ownershipTypeService;

  @Autowired
  @Qualifier("settingsService")
  private SettingsService settingsService;

  @Autowired
  @Qualifier("lineageService")
  private LineageService lineageService;

  @Autowired
  @Qualifier("queryService")
  private QueryService queryService;

  @Autowired
  @Qualifier("erModelRelationshipService")
  private ERModelRelationshipService erModelRelationshipService;

  @Autowired
  @Qualifier("dataProductService")
  private DataProductService dataProductService;

  @Autowired
  @Qualifier("formService")
  private FormService formService;

  @Autowired
  @Qualifier("restrictedService")
  private RestrictedService restrictedService;

  @Value("${platformAnalytics.enabled}") // TODO: Migrate to DATAHUB_ANALYTICS_ENABLED
  private Boolean isAnalyticsEnabled;

  @Bean(name = "graphQLEngine")
  @Nonnull
  protected GraphQLEngine graphQLEngine(
      @Qualifier("entityClient") final EntityClient entityClient,
      @Qualifier("systemEntityClient") final SystemEntityClient systemEntityClient) {
    GmsGraphQLEngineArgs args = new GmsGraphQLEngineArgs();
    args.setEntityClient(entityClient);
    args.setSystemEntityClient(systemEntityClient);
    args.setGraphClient(graphClient);
    args.setUsageClient(usageClient);
    if (isAnalyticsEnabled) {
      args.setAnalyticsService(new AnalyticsService(elasticClient, indexConvention));
    }
    args.setEntityService(entityService);
    args.setRecommendationsService(recommendationsService);
    args.setStatefulTokenService(statefulTokenService);
    args.setTimeseriesAspectService(timeseriesAspectService);
    args.setEntityRegistry(entityRegistry);
    args.setSecretService(secretService);
    args.setNativeUserService(nativeUserService);
    args.setIngestionConfiguration(configProvider.getIngestion());
    args.setAuthenticationConfiguration(configProvider.getAuthentication());
    args.setAuthorizationConfiguration(configProvider.getAuthorization());
    args.setGitVersion(gitVersion);
    args.setTimelineService(timelineService);
    args.setSupportsImpactAnalysis(graphService.supportsMultiHop());
    args.setVisualConfiguration(configProvider.getVisualConfig());
    args.setTelemetryConfiguration(configProvider.getTelemetry());
    args.setTestsConfiguration(configProvider.getMetadataTests());
    args.setDatahubConfiguration(configProvider.getDatahub());
    args.setViewsConfiguration(configProvider.getViews());
    args.setSiblingGraphService(siblingGraphService);
    args.setGroupService(groupService);
    args.setRoleService(roleService);
    args.setInviteTokenService(inviteTokenService);
    args.setPostService(postService);
    args.setViewService(viewService);
    args.setOwnershipTypeService(ownershipTypeService);
    args.setSettingsService(settingsService);
    args.setLineageService(lineageService);
    args.setQueryService(queryService);
    args.setErModelRelationshipService(erModelRelationshipService);
    args.setFeatureFlags(configProvider.getFeatureFlags());
    args.setFormService(formService);
    args.setRestrictedService(restrictedService);
    args.setDataProductService(dataProductService);
    args.setGraphQLQueryComplexityLimit(
        configProvider.getGraphQL().getQuery().getComplexityLimit());
    args.setGraphQLQueryDepthLimit(configProvider.getGraphQL().getQuery().getDepthLimit());
    return new GmsGraphQLEngine(args).builder().build();
  }
}
