package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Simple interface for classes capable of mapping an input of type I to an output of type O. */
public interface ModelMapper<I, O> {
  O apply(@Nullable final QueryContext context, @Nonnull final I input);
}
