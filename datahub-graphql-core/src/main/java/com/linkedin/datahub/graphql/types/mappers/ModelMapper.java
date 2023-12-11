package com.linkedin.datahub.graphql.types.mappers;

/** Simple interface for classes capable of mapping an input of type I to an output of type O. */
public interface ModelMapper<I, O> {
  O apply(final I input);
}
