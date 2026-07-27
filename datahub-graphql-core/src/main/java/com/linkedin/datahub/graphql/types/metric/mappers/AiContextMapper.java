package com.linkedin.datahub.graphql.types.metric.mappers;

import com.linkedin.datahub.graphql.generated.AiContext;
import java.util.ArrayList;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps {@link com.linkedin.metric.AiContext} Pegasus records to the generated GraphQL {@link
 * AiContext}.
 */
public class AiContextMapper {

  private AiContextMapper() {}

  @Nullable
  public static AiContext map(@Nullable com.linkedin.metric.AiContext pdl) {
    if (pdl == null) {
      return null;
    }

    AiContext result = new AiContext();

    if (pdl.hasSynonyms() && pdl.getSynonyms() != null) {
      result.setSynonyms(new ArrayList<>(pdl.getSynonyms()));
    }
    if (pdl.hasInstructions() && pdl.getInstructions() != null) {
      result.setInstructions(pdl.getInstructions());
    }
    if (pdl.hasExamples() && pdl.getExamples() != null) {
      result.setExamples(new ArrayList<>(pdl.getExamples()));
    }
    if (pdl.hasCustomInstructions() && pdl.getCustomInstructions() != null) {
      result.setCustomInstructions(pdl.getCustomInstructions());
    }

    return result;
  }

  @Nonnull
  public static AiContext mapNonNull(@Nonnull com.linkedin.metric.AiContext pdl) {
    AiContext result = map(pdl);
    return result != null ? result : new AiContext();
  }
}
