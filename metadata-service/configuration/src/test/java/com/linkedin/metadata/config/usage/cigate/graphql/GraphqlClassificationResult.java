package com.linkedin.metadata.config.usage.cigate.graphql;

import io.datahubproject.metadata.context.usage.UsageOperation;
import javax.annotation.Nonnull;

public record GraphqlClassificationResult(
    @Nonnull UsageOperation operation, @Nonnull GraphqlClassificationSource source) {

  boolean isAccountedFor() {
    return source != GraphqlClassificationSource.DEFAULT_QUERY;
  }
}
