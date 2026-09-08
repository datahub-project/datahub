package com.linkedin.gms.factory.common;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.graph.GraphPostgresBackendCondition;
import com.linkedin.gms.factory.graph.PgGraphBackendGuard;
import com.linkedin.gms.factory.graph.PgGraphEbeanConfigFactory;
import com.linkedin.metadata.config.postgres.DatabaseType;
import com.linkedin.metadata.config.postgres.JdbcUrlParser;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.postgres.PostgresGraphLineageDao;
import com.linkedin.metadata.graph.postgres.PostgresGraphOneHopDao;
import com.linkedin.metadata.graph.postgres.PostgresGraphPgRoutingDao;
import com.linkedin.metadata.graph.postgres.PostgresGraphService;
import com.linkedin.metadata.graph.postgres.PostgresGraphTables;
import com.linkedin.metadata.graph.postgres.PostgresGraphWriteDao;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.LineageRegistry;
import io.ebean.Database;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Conditional(GraphPostgresBackendCondition.class)
@Import({PgGraphEbeanConfigFactory.class, PgGraphBackendGuard.class})
public class PostgresGraphServiceFactory {

  @Bean(name = "graphService")
  @Nonnull
  public GraphService postgresGraphService(
      final EntityRegistry entityRegistry,
      final ConfigurationProvider configurationProvider,
      @Qualifier("pgGraphEbeanServer") ObjectProvider<Database> databaseProvider,
      final PostgresSqlSetupProperties postgresSqlSetupProperties,
      @Value("${postgres.pgGraph.pool.url:}") String graphPoolUrl) {

    if (!postgresSqlSetupProperties.getPgGraph().isEnabled()) {
      throw new IllegalStateException(
          "graphService.type=postgres requires postgres.pgGraph.enabled=true"
              + " (DATAHUB_PGGRAPH_ENABLED=true)");
    }
    Database database = databaseProvider.getIfAvailable();
    if (database == null) {
      throw new IllegalStateException(
          "graphService.type=postgres but pgGraphEbeanServer is not available; set"
              + " postgres.pgGraph.enabled=true with a PostgreSQL postgres.pgGraph.pool.url"
              + " (or ebean.url)");
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
    PostgresGraphWriteDao writeDao =
        new PostgresGraphWriteDao(database, postgresSqlSetupProperties);
    PostgresGraphTables tables = new PostgresGraphTables(postgresSqlSetupProperties);
    PostgresGraphOneHopDao oneHopDao = new PostgresGraphOneHopDao(database, tables);
    PostgresGraphPgRoutingDao pgRoutingDao =
        new PostgresGraphPgRoutingDao(database, tables, lineageRegistry);
    PostgresGraphLineageDao lineageDao =
        new PostgresGraphLineageDao(
            oneHopDao, pgRoutingDao, lineageRegistry, configurationProvider.getGraphService());
    PostgresGraphService service =
        new PostgresGraphService(
            configurationProvider.getGraphService(),
            lineageRegistry,
            writeDao,
            oneHopDao,
            lineageDao);
    if (configurationProvider.getDatahub().isReadOnly()) {
      service.setWritable(false);
    }
    return service;
  }
}
