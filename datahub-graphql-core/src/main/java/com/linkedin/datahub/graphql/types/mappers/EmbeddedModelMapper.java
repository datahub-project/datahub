package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Made for models that are embedded in other models and thus do not encode their own URN. */
public interface EmbeddedModelMapper<I, O> {
  O apply(
      @Nullable final QueryContext context, @Nonnull final I input, @Nonnull final Urn entityUrn);
}
