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
import com.linkedin.gms.factory.auth.DataHubTokenServiceFactory;
import com.linkedin.gms.factory.common.GitVersionFactory;
import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.common.RestHighLevelClientFactory;
import com.linkedin.gms.factory.common.SiblingGraphServiceFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entity.RestliEntityClientFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.recommendation.RecommendationServiceFactory;
import com.linkedin.metadata.client.JavaEntityClient;
import com.linkedin.metadata.client.SystemJavaEntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.SiblingGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.recommendation.RecommendationsService;
import com.linkedin.metadata.secret.SecretService;
import com.linkedin.metadata.service.DataProductService;
import com.linkedin.metadata.service.LineageService;
import com.linkedin.metadata.service.OwnershipTypeService;
import com.linkedin.metadata.service.QueryService;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.usage.UsageClient;
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
  RestliEntityClientFactory.class,
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
  @Qualifier("javaEntityClient")
  private JavaEntityClient _entityClient;

  @Autowired
  @Qualifier("systemJavaEntityClient")
  private SystemJavaEntityClient _systemEntityClient;

  @Autowired
  @Qualifier("graphClient")
  private GraphClient _graphClient;

  @Autowired
  @Qualifier("usageClient")
  private UsageClient _usageClient;

  @Autowired
  @Qualifier("entityService")
  private EntityService _entityService;

  @Autowired
  @Qualifier("graphService")
  private GraphService _graphService;

  @Autowired
  @Qualifier("siblingGraphService")
  private SiblingGraphService _siblingGraphService;

  @Autowired
  @Qualifier("timeseriesAspectService")
  private TimeseriesAspectService _timeseriesAspectService;

  @Autowired
  @Qualifier("recommendationsService")
  private RecommendationsService _recommendationsService;

  @Autowired
  @Qualifier("dataHubTokenService")
  private StatefulTokenService _statefulTokenService;

  @Autowired
  @Qualifier("dataHubSecretService")
  private SecretService _secretService;

  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry _entityRegistry;

  @Autowired
  @Qualifier("configurationProvider")
  private ConfigurationProvider _configProvider;

  @Autowired
  @Qualifier("gitVersion")
  private GitVersion _gitVersion;

  @Autowired
  @Qualifier("timelineService")
  private TimelineService _timelineService;

  @Autowired
  @Qualifier("nativeUserService")
  private NativeUserService _nativeUserService;

  @Autowired
  @Qualifier("groupService")
  private GroupService _groupService;

  @Autowired
  @Qualifier("roleService")
  private RoleService _roleService;

  @Autowired
  @Qualifier("inviteTokenService")
  private InviteTokenService _inviteTokenService;

  @Autowired
  @Qualifier("postService")
  private PostService _postService;

  @Autowired
  @Qualifier("viewService")
  private ViewService _viewService;

  @Autowired
  @Qualifier("ownerShipTypeService")
  private OwnershipTypeService _ownershipTypeService;

  @Autowired
  @Qualifier("settingsService")
  private SettingsService _settingsService;

  @Autowired
  @Qualifier("lineageService")
  private LineageService _lineageService;

  @Autowired
  @Qualifier("queryService")
  private QueryService _queryService;

  @Autowired
  @Qualifier("dataProductService")
  private DataProductService _dataProductService;

  @Value("${platformAnalytics.enabled}") // TODO: Migrate to DATAHUB_ANALYTICS_ENABLED
  private Boolean isAnalyticsEnabled;

  @Bean(name = "graphQLEngine")
  @Nonnull
  protected GraphQLEngine getInstance() {
    GmsGraphQLEngineArgs args = new GmsGraphQLEngineArgs();
    args.setEntityClient(_entityClient);
    args.setSystemEntityClient(_systemEntityClient);
    args.setGraphClient(_graphClient);
    args.setUsageClient(_usageClient);
    if (isAnalyticsEnabled) {
      args.setAnalyticsService(new AnalyticsService(elasticClient, indexConvention));
    }
    args.setEntityService(_entityService);
    args.setRecommendationsService(_recommendationsService);
    args.setStatefulTokenService(_statefulTokenService);
    args.setTimeseriesAspectService(_timeseriesAspectService);
    args.setEntityRegistry(_entityRegistry);
    args.setSecretService(_secretService);
    args.setNativeUserService(_nativeUserService);
    args.setIngestionConfiguration(_configProvider.getIngestion());
    args.setAuthenticationConfiguration(_configProvider.getAuthentication());
    args.setAuthorizationConfiguration(_configProvider.getAuthorization());
    args.setGitVersion(_gitVersion);
    args.setTimelineService(_timelineService);
    args.setSupportsImpactAnalysis(_graphService.supportsMultiHop());
    args.setVisualConfiguration(_configProvider.getVisualConfig());
    args.setTelemetryConfiguration(_configProvider.getTelemetry());
    args.setTestsConfiguration(_configProvider.getMetadataTests());
    args.setDatahubConfiguration(_configProvider.getDatahub());
    args.setViewsConfiguration(_configProvider.getViews());
    args.setSiblingGraphService(_siblingGraphService);
    args.setGroupService(_groupService);
    args.setRoleService(_roleService);
    args.setInviteTokenService(_inviteTokenService);
    args.setPostService(_postService);
    args.setViewService(_viewService);
    args.setOwnershipTypeService(_ownershipTypeService);
    args.setSettingsService(_settingsService);
    args.setLineageService(_lineageService);
    args.setQueryService(_queryService);
    args.setFeatureFlags(_configProvider.getFeatureFlags());
    args.setDataProductService(_dataProductService);
    return new GmsGraphQLEngine(args).builder().build();
  }
}
