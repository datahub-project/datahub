package io.datahubproject.metadata.context;

import java.util.Optional;

public class EmptyContext implements ContextInterface {
  public static final EmptyContext EMPTY = new EmptyContext();

  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return Optional.empty();
  }
}
