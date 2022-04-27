package com.linkedin.gms.factory.graphql;

import com.datahub.authentication.token.TokenService;
import com.linkedin.datahub.graphql.GmsGraphQLEngine;
import com.linkedin.datahub.graphql.GraphQLEngine;
import com.linkedin.datahub.graphql.analytics.service.AnalyticsService;
import com.linkedin.datahub.graphql.generated.VisualConfiguration;
import com.linkedin.entity.client.JavaEntityClient;
import com.linkedin.gms.factory.auth.DataHubTokenServiceFactory;
import com.linkedin.gms.factory.common.GitVersionFactory;
import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.common.RestHighLevelClientFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.entity.RestliEntityClientFactory;
import com.linkedin.gms.factory.recommendation.RecommendationServiceFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.recommendation.RecommendationsService;
import com.linkedin.metadata.secret.SecretService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.usage.UsageClient;
import javax.annotation.Nonnull;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;


@Configuration
@Import({RestHighLevelClientFactory.class, IndexConventionFactory.class, RestliEntityClientFactory.class,
    RecommendationServiceFactory.class, EntityRegistryFactory.class, DataHubTokenServiceFactory.class,
    GitVersionFactory.class})
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
  @Qualifier("timeseriesAspectService")
  private TimeseriesAspectService _timeseriesAspectService;

  @Autowired
  private RecommendationsService _recommendationsService;

  @Autowired
  @Qualifier("dataHubTokenService")
  private TokenService _tokenService;

  @Autowired
  @Qualifier("dataHubSecretService")
  private SecretService _secretService;

  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry _entityRegistry;

  @Autowired
  private ConfigurationProvider _configProvider;

  @Autowired
  @Qualifier("gitVersion")
  private GitVersion _gitVersion;

  @Autowired
  @Qualifier("visualConfig")
  private VisualConfiguration _visualConfiguration;

  @Value("${platformAnalytics.enabled}") // TODO: Migrate to DATAHUB_ANALYTICS_ENABLED
  private Boolean isAnalyticsEnabled;

  @Bean(name = "graphQLEngine")
  @Nonnull
  protected GraphQLEngine getInstance() {
    if (isAnalyticsEnabled) {
      return new GmsGraphQLEngine(
          _entityClient,
          _graphClient,
          _usageClient,
          new AnalyticsService(elasticClient, indexConvention),
          _entityService,
          _recommendationsService,
          _tokenService,
          _timeseriesAspectService,
          _entityRegistry,
          _secretService,
          _configProvider.getIngestion(),
          _configProvider.getAuthentication(),
          _configProvider.getAuthorization(),
          _gitVersion,
          _graphService.supportsMultiHop(),
          _visualConfiguration,
          _configProvider.getTelemetry()
          ).builder().build();
    }
    return new GmsGraphQLEngine(
        _entityClient,
        _graphClient,
        _usageClient,
        null,
        _entityService,
        _recommendationsService,
        _tokenService,
        _timeseriesAspectService,
        _entityRegistry,
        _secretService,
        _configProvider.getIngestion(),
        _configProvider.getAuthentication(),
        _configProvider.getAuthorization(),
        _gitVersion,
        _graphService.supportsMultiHop(),
        _visualConfiguration,
        _configProvider.getTelemetry()
    ).builder().build();
  }
}
