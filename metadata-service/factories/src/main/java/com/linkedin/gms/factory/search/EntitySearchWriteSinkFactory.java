package com.linkedin.gms.factory.search;

import com.linkedin.metadata.config.postgres.DatabaseType;
import com.linkedin.metadata.config.postgres.JdbcUrlParser;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.search.postgres.PostgresEntitySearchWriteSink;
import com.linkedin.metadata.search.write.EntitySearchWriteSink;
import io.ebean.Database;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Slf4j
@Import(PgSearchEbeanConfigFactory.class)
public class EntitySearchWriteSinkFactory {

  @Bean
  @Nonnull
  public EntitySearchWriteSink entitySearchWriteSink(
      @Qualifier("pgSearchEbeanServer") ObjectProvider<Database> pgSearchDatabaseProvider,
      ObjectProvider<Database> databaseProvider,
      PostgresSqlSetupProperties postgresSqlSetupProperties,
      @Value("${ebean.url:}") String ebeanUrl) {
    if (!postgresSqlSetupProperties.getPgSearch().getEntity().isEnabled()) {
      return EntitySearchWriteSink.NOOP;
    }
    Database database = pgSearchDatabaseProvider.getIfAvailable();
    if (database == null) {
      database = databaseProvider.getIfAvailable();
    }
    if (database == null) {
      log.warn(
          "postgres.pgSearch.entity.enabled but no pgSearchEbeanServer or Ebean Database bean is available; skipping PostgreSQL entity search dual-write");
      return EntitySearchWriteSink.NOOP;
    }
    if (ebeanUrl == null || ebeanUrl.isBlank()) {
      log.warn(
          "postgres.pgSearch.entity.enabled but ebean.url is empty; skipping PostgreSQL entity search dual-write");
      return EntitySearchWriteSink.NOOP;
    }
    try {
      JdbcUrlParser.JdbcInfo info = JdbcUrlParser.parseJdbcUrl(ebeanUrl.trim());
      if (info.databaseType != DatabaseType.POSTGRES) {
        log.warn(
            "postgres.pgSearch.entity.enabled but ebean.url is not PostgreSQL; skipping PostgreSQL entity search dual-write");
        return EntitySearchWriteSink.NOOP;
      }
      postgresSqlSetupProperties.applySqlSetupSchemaFromJdbcUrl(ebeanUrl);
      postgresSqlSetupProperties.validateForUse(DatabaseType.POSTGRES);
    } catch (IllegalArgumentException | IllegalStateException e) {
      log.warn(
          "postgres.pgSearch.entity.enabled but Postgres SqlSetup validation failed ({}); skipping PostgreSQL entity search dual-write",
          e.getMessage());
      return EntitySearchWriteSink.NOOP;
    }
    return new PostgresEntitySearchWriteSink(database, postgresSqlSetupProperties);
  }
}
