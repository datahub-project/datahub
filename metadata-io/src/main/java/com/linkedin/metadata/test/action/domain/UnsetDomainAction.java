package com.linkedin.metadata.test.action.domain;

import static com.linkedin.metadata.Constants.METADATA_TESTS_SOURCE;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.service.DomainServiceAsync;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.action.api.NoValidationAction;
import com.linkedin.metadata.test.definition.ActionType;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class UnsetDomainAction extends NoValidationAction {

  private final DomainServiceAsync domainService;

  @Override
  public ActionType getActionType() {
    return ActionType.UNSET_DOMAIN;
  }

  @Override
  public void apply(@Nonnull OperationContext opContext, List<Urn> urns, ActionParameters params)
      throws InvalidOperandException {
    if (urns.isEmpty()) return;

    try {
      // Unset ALL domains from the entities (no specific domain URN needed)
      this.domainService.batchUnsetDomain(
          opContext, this.getResourceReferences(urns), METADATA_TESTS_SOURCE);
      log.info("Successfully unset domains for {} entities", urns.size());
    } catch (Exception e) {
      log.error("Failed to unset domains for entities: {}", e.getMessage(), e);
      throw new InvalidOperandException("Failed to unset domains: " + e.getMessage(), e);
    }
  }
}
