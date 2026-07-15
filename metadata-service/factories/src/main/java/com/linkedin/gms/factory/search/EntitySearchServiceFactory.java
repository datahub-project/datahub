package com.linkedin.gms.factory.search;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.postgres.DatabaseType;
import com.linkedin.metadata.config.postgres.JdbcUrlParser;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.RoutingEntitySearchService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.postgres.PostgresEntitySearchService;
import io.ebean.Database;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@Configuration
@Import({ElasticsearchEntitySearchSupportConfiguration.class, PgSearchEbeanConfigFactory.class})
public class EntitySearchServiceFactory {
  @Bean(name = "entitySearchService")
  @Primary
  @Nonnull
  protected EntitySearchService getInstance(
      @Qualifier("pgSearchEbeanServer") ObjectProvider<Database> pgSearchDatabaseProvider,
      ObjectProvider<Database> databaseProvider,
      ObjectProvider<ElasticSearchService> elasticSearchServiceProvider,
      PostgresSqlSetupProperties postgresSqlSetupProperties,
      ConfigurationProvider configurationProvider,
      @Value("${ebean.url:}") String ebeanUrl) {
    ElasticSearchConfiguration esConfig = configurationProvider.getElasticSearch();
    boolean elasticsearchIntegrationOn = esConfig != null && esConfig.isEnabled();
    if (!elasticsearchIntegrationOn
        && !postgresSqlSetupProperties.getPgSearch().getEntity().getQuery().isEnabled()) {
      throw new IllegalStateException(
          "elasticsearch.enabled=false requires postgres.pgSearch.entity.query.enabled=true "
              + "(PostgreSQL-backed entity search); otherwise no search backend is configured.");
    }
    if (!postgresSqlSetupProperties.getPgSearch().getEntity().getQuery().isEnabled()) {
      ElasticSearchService elasticSearchOnly = elasticSearchServiceProvider.getIfAvailable();
      if (elasticSearchOnly == null) {
        throw new IllegalStateException(
            "PostgreSQL entity search query is disabled but no ElasticSearchService bean is "
                + "registered; enable elasticsearch.enabled=true or postgres.pgSearch.entity.query.enabled=true.");
      }
      return elasticSearchOnly;
    }
    if (!postgresSqlSetupProperties.getPgSearch().getEntity().isEnabled()) {
      throw new IllegalStateException(
          "postgres.pgSearch.entity.query.enabled requires postgres.pgSearch.entity.enabled=true");
    }
    Database database = pgSearchDatabaseProvider.getIfAvailable();
    if (database == null) {
      database = databaseProvider.getIfAvailable();
    }
    if (database == null) {
      throw new IllegalStateException(
          "postgres.pgSearch.entity.query.enabled is true but no pgSearchEbeanServer or Ebean Database bean is available");
    }
    if (ebeanUrl == null || ebeanUrl.isBlank()) {
      throw new IllegalStateException(
          "postgres.pgSearch.entity.query.enabled is true but ebean.url is not set");
    }
    JdbcUrlParser.JdbcInfo info = JdbcUrlParser.parseJdbcUrl(ebeanUrl.trim());
    if (info.databaseType != DatabaseType.POSTGRES) {
      throw new IllegalStateException(
          "postgres.pgSearch.entity.query.enabled is true but ebean.url is not a PostgreSQL JDBC URL");
    }
    postgresSqlSetupProperties.applySqlSetupSchemaFromJdbcUrl(ebeanUrl);
    postgresSqlSetupProperties.validateForUse(DatabaseType.POSTGRES);
    PostgresEntitySearchService pg =
        new PostgresEntitySearchService(
            database, postgresSqlSetupProperties, configurationProvider.getSearchService());
    if (!elasticsearchIntegrationOn) {
      return pg;
    }
    ElasticSearchService elasticForRouting = elasticSearchServiceProvider.getIfAvailable();
    if (elasticForRouting == null) {
      throw new IllegalStateException(
          "Elasticsearch integration is enabled but ElasticSearchService is not registered.");
    }
    return new RoutingEntitySearchService(
        elasticForRouting, pg, configurationProvider.getSearchService());
  }
}
