package com.linkedin.gms.factory.search.semantic;

import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.semantic.SearchableFieldExtractor;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SearchableFieldExtractorFactory {

  @Bean(name = "searchableFieldExtractor")
  @Nonnull
  protected SearchableFieldExtractor getInstance(@Nonnull final EntityRegistry entityRegistry) {
    return new SearchableFieldExtractor(entityRegistry);
  }
}
