package com.linkedin.gms.factory.search;

import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.V3DocumentIdScheme;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * The document-id scheme recorded on V3 indices at creation and checked by the incremental upgrade,
 * derived from whichever {@link EntityDocumentIdHasher} the deployment wires.
 */
@Configuration
@Import(EntityDocumentIdHasherFactory.class)
public class V3DocumentIdSchemeFactory {

  @Bean("v3DocumentIdScheme")
  @Nonnull
  protected V3DocumentIdScheme v3DocumentIdScheme(final EntityDocumentIdHasher hasher) {
    return new V3DocumentIdScheme(hasher);
  }
}
