package io.datahubproject.metadata.context;

import java.util.Optional;
import lombok.Builder;
import lombok.Getter;

/** Context holder for environment variables relevant to operations */
@Builder
@Getter
public class ValidationContext implements ContextInterface {
  // Uses alternate validation flow for MCP ingestion
  private final boolean alternateValidation;

  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return Optional.of(alternateValidation ? 1 : 0);
  }
}
