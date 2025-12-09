/**
 * SAAS-SPECIFIC: This factory is part of the semantic search feature exclusive to DataHub SaaS. It
 * should NOT be merged back to the open-source DataHub repository. Dependencies: Creates embedding
 * provider that connects to DataHub Integrations Service.
 */
package com.linkedin.gms.factory.search.semantic;

import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.embedding.IntegrationsServiceEmbeddingProvider;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

@Configuration
public class EmbeddingProviderFactory {

  @Autowired
  @Lazy
  @Qualifier("integrationsService")
  private IntegrationsService integrationsService;

  @Bean(name = "embeddingProvider")
  @Nonnull
  protected EmbeddingProvider getInstance() {
    return new IntegrationsServiceEmbeddingProvider(integrationsService);
  }
}
