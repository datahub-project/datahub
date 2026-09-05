package com.linkedin.gms.factory.systemmetadata;

import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.systemmetadata.scroll.PostgresSystemMetadataScrollClient;
import io.ebean.Database;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PostgresSystemMetadataScrollClientFactory {

  @Bean(name = "postgresSystemMetadataScrollClient")
  @Nonnull
  @Conditional(PgSystemMetadataRuntimePoolEnabledCondition.class)
  public PostgresSystemMetadataScrollClient postgresSystemMetadataScrollClient(
      @Qualifier("pgSystemMetadataEbeanServer") ObjectProvider<Database> databaseProvider,
      PostgresSqlSetupProperties postgresSqlSetupProperties) {
    Database database = databaseProvider.getIfAvailable();
    if (database == null) {
      throw new IllegalStateException(
          "pgSystemMetadata scroll client requires pgSystemMetadataEbeanServer");
    }
    return new PostgresSystemMetadataScrollClient(database, postgresSqlSetupProperties);
  }
}
