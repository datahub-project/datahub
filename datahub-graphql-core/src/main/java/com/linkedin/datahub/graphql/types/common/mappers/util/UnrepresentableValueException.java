package com.linkedin.datahub.graphql.types.common.mappers.util;

import javax.annotation.Nonnull;

/**
 * A stored value GraphQL can't show truthfully, such as an enum value only a newer version knows
 * (read after a rollback). Callers leave out the object that holds it rather than show a guess that
 * could be saved back. It extends {@link IllegalArgumentException}, which {@link Enum#valueOf}
 * threw before, so one left uncaught fails the request as it did.
 */
public class UnrepresentableValueException extends IllegalArgumentException {

  public UnrepresentableValueException(
      @Nonnull final Class<?> graphqlType, @Nonnull final Object storedValue) {
    super(
        String.format(
            "%s has no value for %s, which only a newer version knows",
            graphqlType.getSimpleName(), storedValue));
  }
}
