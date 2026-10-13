package com.linkedin.gms.factory.search;

import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import javax.annotation.Nonnull;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EntityDocumentIdHasherFactory {

  @Bean
  @ConditionalOnMissingBean(EntityDocumentIdHasher.class)
  @Nonnull
  protected EntityDocumentIdHasher entityDocumentIdHasher() {
    return new Sha256UrnEntityDocumentIdHasher();
  }
}
