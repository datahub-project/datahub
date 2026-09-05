package com.linkedin.gms.factory.systemmetadata;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.systemmetadata.PostgresSystemMetadataService;
import io.ebean.Database;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PostgresSystemMetadataServiceConfiguration {

  @Bean
  @Nonnull
  @Conditional(SystemMetadataPostgresBackendCondition.class)
  public PostgresSystemMetadataService postgresSystemMetadataService(
      @Qualifier("pgSystemMetadataEbeanServer") ObjectProvider<Database> databaseProvider,
      PostgresSqlSetupProperties postgresSqlSetupProperties,
      ConfigurationProvider configurationProvider,
      @Value("${elasticsearch.idHashAlgo}") final String elasticIdHashAlgo) {
    if (!postgresSqlSetupProperties.getPgSystemMetadata().isEnabled()) {
      throw new IllegalStateException(
          "systemMetadataService.implementation=postgres requires"
              + " postgres.pgSystemMetadata.enabled=true (DATAHUB_PGSYSTEMMETADATA_ENABLED=true)");
    }
    Database database = databaseProvider.getIfAvailable();
    if (database == null) {
      throw new IllegalStateException(
          "systemMetadataService.implementation=postgres but pgSystemMetadataEbeanServer is not"
              + " available; set postgres.pgSystemMetadata.enabled=true with a PostgreSQL"
              + " postgres.pgSystemMetadata.pool.url (or ebean.url)");
    }
    return new PostgresSystemMetadataService(
        database,
        postgresSqlSetupProperties,
        configurationProvider.getSystemMetadataService(),
        elasticIdHashAlgo);
  }
}
