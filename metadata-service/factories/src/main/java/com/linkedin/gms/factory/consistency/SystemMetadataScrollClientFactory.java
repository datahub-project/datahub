package com.linkedin.gms.factory.consistency;

import com.linkedin.metadata.systemmetadata.scroll.ESSystemMetadataScrollClient;
import com.linkedin.metadata.systemmetadata.scroll.PostgresSystemMetadataScrollClient;
import com.linkedin.metadata.systemmetadata.scroll.SystemMetadataScrollClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

/**
 * Selects the active {@link SystemMetadataScrollClient} bean based on which backend factory is
 * present.
 *
 * <p>Mirrors the pattern used by {@link
 * com.linkedin.gms.factory.common.SystemMetadataServiceFactory}: both backend factories are
 * imported and gated by {@code elasticsearch.enabled}; this selector picks whichever produced a
 * bean. This keeps the {@link com.linkedin.metadata.aspect.consistency.ConsistencyService} (and its
 * consumers) backend-agnostic.
 */
@Configuration
@Import({
  ESSystemMetadataScrollClientFactory.class,
  PostgresSystemMetadataScrollClientFactory.class
})
public class SystemMetadataScrollClientFactory {

  @Bean(name = "systemMetadataScrollClient")
  @Primary
  @Nonnull
  public SystemMetadataScrollClient systemMetadataScrollClient(
      @Autowired(required = false) @Qualifier("esSystemMetadataScrollClient")
          final ESSystemMetadataScrollClient esClient,
      @Autowired(required = false) @Qualifier("postgresSystemMetadataScrollClient")
          final PostgresSystemMetadataScrollClient postgresClient) {
    if (esClient != null) {
      return esClient;
    }
    if (postgresClient != null) {
      return postgresClient;
    }
    throw new IllegalStateException(
        "No SystemMetadataScrollClient bean is available; check elasticsearch.enabled and "
            + "PostgreSQL system metadata wiring.");
  }
}
