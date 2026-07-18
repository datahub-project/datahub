package com.linkedin.gms.factory.graph;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.postgres.DatabaseType;
import com.linkedin.metadata.config.postgres.JdbcUrlParser;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.graph.postgres.PostgresGraphWriteSink;
import com.linkedin.metadata.graph.write.GraphWriteSink;
import io.ebean.Database;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class GraphWriteSinkFactory {

  @Bean
  @Nonnull
  public GraphWriteSink graphWriteSink(
      @Qualifier("pgGraphEbeanServer") ObjectProvider<Database> databaseProvider,
      PostgresSqlSetupProperties postgresSqlSetupProperties,
      ConfigurationProvider configurationProvider,
      @Value("${postgres.pgGraph.pool.url:}") String graphPoolUrl,
      @Value("${graphService.type:elasticsearch}") String graphServiceType) {
    if ("postgres".equalsIgnoreCase(graphServiceType)) {
      return GraphWriteSink.NOOP;
    }
    if (!postgresSqlSetupProperties.getPgGraph().isEnabled()) {
      return GraphWriteSink.NOOP;
    }
    Database database = databaseProvider.getIfAvailable();
    if (database == null) {
      log.warn(
          "postgres.pgGraph.enabled but no pgGraphEbeanServer bean is available; skipping PostgreSQL graph persistence");
      return GraphWriteSink.NOOP;
    }
    if (graphPoolUrl == null || graphPoolUrl.isBlank()) {
      log.warn(
          "postgres.pgGraph.enabled but postgres.pgGraph.pool.url is empty; skipping PostgreSQL graph persistence");
      return GraphWriteSink.NOOP;
    }
    try {
      JdbcUrlParser.JdbcInfo info = JdbcUrlParser.parseJdbcUrl(graphPoolUrl.trim());
      if (info.databaseType != DatabaseType.POSTGRES) {
        log.warn(
            "postgres.pgGraph.enabled but postgres.pgGraph.pool.url is not PostgreSQL; skipping PostgreSQL graph persistence");
        return GraphWriteSink.NOOP;
      }
      postgresSqlSetupProperties.applySqlSetupSchemaFromJdbcUrl(graphPoolUrl);
      postgresSqlSetupProperties.validateForUse(DatabaseType.POSTGRES);
    } catch (IllegalArgumentException | IllegalStateException e) {
      log.warn(
          "postgres.pgGraph.enabled but Postgres SqlSetup validation failed ({}); skipping PostgreSQL graph persistence",
          e.getMessage());
      return GraphWriteSink.NOOP;
    }
    return new PostgresGraphWriteSink(
        database, postgresSqlSetupProperties, configurationProvider.getGraphService());
  }
}
