package com.linkedin.datahub.graphql.types.semanticmodel.mappers;

import com.linkedin.datahub.graphql.generated.SemanticModelProperties;
import javax.annotation.Nonnull;

/**
 * Maps the {@link com.linkedin.dataset.SemanticModelProperties} Pegasus aspect (attached to a
 * {@code dataset} entity carrying the {@code Semantic Model Dataset} subtype) to the generated
 * GraphQL {@link SemanticModelProperties}. Containment lives on {@code semanticModelInfo.datasets};
 * this aspect only carries the local alias.
 */
public class SemanticModelPropertiesMapper {

  private SemanticModelPropertiesMapper() {}

  @Nonnull
  public static SemanticModelProperties map(
      @Nonnull final com.linkedin.dataset.SemanticModelProperties pdl) {
    final SemanticModelProperties result = new SemanticModelProperties();
    result.setAlias(pdl.getAlias());
    return result;
  }
}
