package com.linkedin.gms.factory.common;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.systemmetadata.PostgresSystemMetadataService;
import io.ebean.Database;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(prefix = "elasticsearch", name = "enabled", havingValue = "false")
@ConditionalOnProperty(prefix = "postgres.pgSystemMetadata", name = "enabled", havingValue = "true")
public class PostgresSystemMetadataServiceFactory {

  @Bean(name = "postgresSystemMetadataService")
  @Nonnull
  public PostgresSystemMetadataService postgresSystemMetadataService(
      @Qualifier("ebeanServer") final Database database,
      final PostgresSqlSetupProperties postgresSqlSetupProperties,
      final ConfigurationProvider configurationProvider,
      @Value("${elasticsearch.idHashAlgo}") final String elasticIdHashAlgo) {

    return new PostgresSystemMetadataService(
        database,
        postgresSqlSetupProperties,
        configurationProvider.getSystemMetadataService(),
        elasticIdHashAlgo);
  }
}
