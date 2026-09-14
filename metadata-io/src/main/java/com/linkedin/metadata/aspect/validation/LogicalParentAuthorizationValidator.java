package com.linkedin.metadata.aspect.validation;

import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.logical.LogicalParent;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.EntityAspectAuthorizationUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
@Getter
@Accessors(chain = true)
public class LogicalParentAuthorizationValidator extends AbstractAspectAuthorizationValidator {

  @Nonnull private AspectPluginConfig config;

  @Override
  protected List<AspectValidationException> validateItems(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull List<? extends BatchItem> items,
      @Nonnull Collection<? extends BatchItem> batchItems,
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull AuthorizationSession session) {

    List<AspectValidationException> failures = new ArrayList<>();
    for (BatchItem item : items) {
      Urn proposedParentUrn = null;
      LogicalParent proposed = item.getAspect(LogicalParent.class);
      if (proposed != null && proposed.hasParent() && proposed.getParent().hasDestinationUrn()) {
        proposedParentUrn = proposed.getParent().getDestinationUrn();
      }
      if (!EntityAspectAuthorizationUtils.isAuthorizedToEditLogicalParent(
          session, item.getUrn(), proposedParentUrn)) {
        failures.add(
            authFailure(item, "Unauthorized to modify logicalParent on entity: " + item.getUrn()));
      }
    }
    return failures;
  }
}
