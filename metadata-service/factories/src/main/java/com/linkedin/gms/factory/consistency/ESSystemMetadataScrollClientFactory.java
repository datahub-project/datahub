package com.linkedin.gms.factory.consistency;

import com.linkedin.metadata.systemmetadata.ESSystemMetadataDAO;
import com.linkedin.metadata.systemmetadata.scroll.ESSystemMetadataScrollClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Provides an Elasticsearch-backed {@link ESSystemMetadataScrollClient} when {@code
 * elasticsearch.enabled=true} (the default).
 *
 * <p>Selected by {@link SystemMetadataScrollClientFactory} when the cluster is configured for
 * Elasticsearch.
 */
@Configuration
@ConditionalOnProperty(
    prefix = "elasticsearch",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
public class ESSystemMetadataScrollClientFactory {

  @Bean(name = "esSystemMetadataScrollClient")
  @Nonnull
  public ESSystemMetadataScrollClient esSystemMetadataScrollClient(
      @Qualifier("esSystemMetadataDAO") final ESSystemMetadataDAO esSystemMetadataDAO) {
    return new ESSystemMetadataScrollClient(esSystemMetadataDAO);
  }
}
