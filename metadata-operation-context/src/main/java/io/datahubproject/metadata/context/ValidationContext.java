package io.datahubproject.metadata.context;

import java.util.Optional;
import lombok.Builder;
import lombok.Getter;

/** Context holder for environment variables relevant to operations */
@Builder(toBuilder = true)
@Getter
public class ValidationContext implements ContextInterface {
  // Uses alternate validation flow for MCP ingestion
  private final boolean alternateValidation;

  // Flag to indicate this is a remediation deletion (skip size validation to prevent circular
  // validation)
  private final boolean isRemediationDeletion;

  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return Optional.of((alternateValidation ? 1 : 0) + (isRemediationDeletion ? 2 : 0));
  }
}
