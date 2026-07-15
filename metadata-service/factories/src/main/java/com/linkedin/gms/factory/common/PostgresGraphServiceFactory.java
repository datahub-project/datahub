package com.linkedin.gms.factory.common;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.postgres.DatabaseType;
import com.linkedin.metadata.config.postgres.JdbcUrlParser;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.postgres.PostgresGraphLineageDao;
import com.linkedin.metadata.graph.postgres.PostgresGraphOneHopDao;
import com.linkedin.metadata.graph.postgres.PostgresGraphService;
import com.linkedin.metadata.graph.postgres.PostgresGraphTables;
import com.linkedin.metadata.graph.postgres.PostgresGraphWriteSink;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.LineageRegistry;
import io.ebean.Database;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "graphService.type", havingValue = "postgres")
public class PostgresGraphServiceFactory {

  @Bean(name = "graphService")
  @Nonnull
  public GraphService postgresGraphService(
      final EntityRegistry entityRegistry,
      final ConfigurationProvider configurationProvider,
      @Qualifier("pgGraphEbeanServer") final Database database,
      final PostgresSqlSetupProperties postgresSqlSetupProperties,
      @org.springframework.beans.factory.annotation.Value("${postgres.pgGraph.pool.url:}")
          String graphPoolUrl) {

    if (!postgresSqlSetupProperties.getPgGraph().isEnabled()) {
      throw new IllegalStateException(
          "graphService.type=postgres requires postgres.pgGraph.enabled=true");
    }
    if (graphPoolUrl == null || graphPoolUrl.isBlank()) {
      throw new IllegalStateException(
          "graphService.type=postgres requires a non-empty postgres.pgGraph.pool.url"
              + " (defaults to ebean.url)");
    }
    JdbcUrlParser.JdbcInfo info = JdbcUrlParser.parseJdbcUrl(graphPoolUrl.trim());
    if (info.databaseType != DatabaseType.POSTGRES) {
      throw new IllegalStateException(
          "graphService.type=postgres requires postgres.pgGraph.pool.url to use PostgreSQL");
    }
    postgresSqlSetupProperties.applySqlSetupSchemaFromJdbcUrl(graphPoolUrl);
    postgresSqlSetupProperties.validateForUse(DatabaseType.POSTGRES);

    LineageRegistry lineageRegistry = new LineageRegistry(entityRegistry);
    PostgresGraphWriteSink writeSink =
        new PostgresGraphWriteSink(
            database, postgresSqlSetupProperties, configurationProvider.getGraphService());
    PostgresGraphTables tables = new PostgresGraphTables(postgresSqlSetupProperties);
    PostgresGraphOneHopDao oneHopDao = new PostgresGraphOneHopDao(database, tables);
    PostgresGraphLineageDao lineageDao =
        new PostgresGraphLineageDao(
            oneHopDao, lineageRegistry, configurationProvider.getGraphService());
    PostgresGraphService service =
        new PostgresGraphService(
            configurationProvider.getGraphService(),
            lineageRegistry,
            writeSink,
            oneHopDao,
            lineageDao);
    if (configurationProvider.getDatahub().isReadOnly()) {
      service.setWritable(false);
    }
    return service;
  }
}
