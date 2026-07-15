package com.linkedin.gms.factory.consistency;

import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.systemmetadata.scroll.PostgresSystemMetadataScrollClient;
import io.ebean.Database;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Provides a PostgreSQL-backed {@link PostgresSystemMetadataScrollClient} when {@code
 * elasticsearch.enabled=false} - i.e. the Postgres-only deployment profile that also activates
 * {@link com.linkedin.gms.factory.common.PostgresSystemMetadataServiceFactory}.
 *
 * <p>Selected by {@link SystemMetadataScrollClientFactory} when Elasticsearch is disabled.
 */
@Configuration
@ConditionalOnProperty(prefix = "elasticsearch", name = "enabled", havingValue = "false")
public class PostgresSystemMetadataScrollClientFactory {

  @Bean(name = "postgresSystemMetadataScrollClient")
  @Nonnull
  public PostgresSystemMetadataScrollClient postgresSystemMetadataScrollClient(
      @Qualifier("ebeanServer") final Database database,
      final PostgresSqlSetupProperties postgresSqlSetupProperties) {
    return new PostgresSystemMetadataScrollClient(database, postgresSqlSetupProperties);
  }
}
