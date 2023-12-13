package com.linkedin.datahub.graphql.types.mappers;

/** Maps an input of type I to an output of type O with actor context. */
public interface InputModelMapper<I, O, A> {
  O apply(final I input, final A actor);
}
