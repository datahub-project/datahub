package com.linkedin.gms.factory.common;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.systemmetadata.PgSystemMetadataBackendGuard;
import com.linkedin.gms.factory.systemmetadata.PgSystemMetadataEbeanConfigFactory;
import com.linkedin.gms.factory.systemmetadata.PostgresSystemMetadataServiceConfiguration;
import com.linkedin.metadata.config.SystemMetadataServiceImplementation;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService;
import com.linkedin.metadata.systemmetadata.PostgresSystemMetadataService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@Configuration
@Import({
  ElasticSearchSystemMetadataServiceFactory.class,
  PostgresSystemMetadataServiceConfiguration.class,
  PgSystemMetadataEbeanConfigFactory.class,
  PgSystemMetadataBackendGuard.class
})
@RequiredArgsConstructor
public class SystemMetadataServiceFactory {

  private final ObjectProvider<ElasticSearchSystemMetadataService>
      elasticSearchSystemMetadataServiceProvider;
  private final ObjectProvider<PostgresSystemMetadataService> postgresSystemMetadataServiceProvider;
  private final ConfigurationProvider configurationProvider;
  private final PostgresSqlSetupProperties postgresSqlSetupProperties;

  @Bean(name = "systemMetadataService")
  @Primary
  @Nonnull
  protected SystemMetadataService createInstance() {
    SystemMetadataServiceImplementation impl =
        configurationProvider.getSystemMetadataService().getImplementation();
    if (impl == null) {
      impl = SystemMetadataServiceImplementation.elasticsearch;
    }
    if (postgresSqlSetupProperties.getPgSystemMetadata().isEnabled()
        && impl != SystemMetadataServiceImplementation.postgres) {
      throw new IllegalStateException(
          "postgres.pgSystemMetadata.enabled=true requires"
              + " systemMetadataService.implementation=postgres; dual-write is not supported");
    }
    if (impl == SystemMetadataServiceImplementation.postgres) {
      PostgresSystemMetadataService pg = postgresSystemMetadataServiceProvider.getIfAvailable();
      if (pg == null) {
        throw new IllegalStateException(
            "systemMetadataService.implementation=postgres requires"
                + " postgres.pgSystemMetadata.enabled=true (DATAHUB_PGSYSTEMMETADATA_ENABLED),"
                + " a PostgreSQL postgres.pgSystemMetadata.pool.url (or ebean.url), and SqlSetup.");
      }
      return pg;
    }
    return elasticSearchSystemMetadataServiceProvider.getObject();
  }
}
