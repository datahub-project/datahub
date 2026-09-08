package com.linkedin.gms.factory.systemmetadata;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.SystemMetadataServiceImplementation;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.systemmetadata.scroll.ESSystemMetadataScrollClient;
import com.linkedin.metadata.systemmetadata.scroll.PostgresSystemMetadataScrollClient;
import com.linkedin.metadata.systemmetadata.scroll.SystemMetadataScrollClient;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@Configuration
@Import({
  ESSystemMetadataScrollClientFactory.class,
  PostgresSystemMetadataScrollClientFactory.class
})
@RequiredArgsConstructor
public class SystemMetadataScrollClientFactory {

  private final ObjectProvider<ESSystemMetadataScrollClient> esScrollClientProvider;
  private final ObjectProvider<PostgresSystemMetadataScrollClient> postgresScrollClientProvider;
  private final ConfigurationProvider configurationProvider;
  private final PostgresSqlSetupProperties postgresSqlSetupProperties;

  @Bean(name = "systemMetadataScrollClient")
  @Primary
  @Nonnull
  public SystemMetadataScrollClient systemMetadataScrollClient() {
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
      PostgresSystemMetadataScrollClient pg = postgresScrollClientProvider.getIfAvailable();
      if (pg == null) {
        throw new IllegalStateException(
            "systemMetadataService.implementation=postgres requires"
                + " postgres.pgSystemMetadata.enabled=true and a pgSystemMetadata Ebean pool");
      }
      return pg;
    }
    return esScrollClientProvider.getObject();
  }
}
