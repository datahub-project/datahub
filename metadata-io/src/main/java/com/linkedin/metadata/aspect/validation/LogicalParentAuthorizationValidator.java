package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.LOGICAL_PARENT_ASPECT_NAME;

import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.logical.LogicalParent;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.EntityAspectAuthorizationUtils;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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

    AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();
    Map<Urn, LogicalParent> stored =
        PatchItemImpl.readStoredForPatches(
            operationContext,
            aspectRetriever,
            items,
            LOGICAL_PARENT_ASPECT_NAME,
            LogicalParent.class);
    List<AspectValidationException> failures = new ArrayList<>();
    for (BatchItem item : items) {
      Urn proposedParentUrn = null;
      LogicalParent proposed;
      try {
        proposed =
            PatchItemImpl.resolveProposedAspect(
                item, stored.get(item.getUrn()), aspectRetriever, LogicalParent.class);
      } catch (RuntimeException e) {
        // logicalParent has no patch template; a PATCH that cannot be applied fails the write too.
        failures.add(authFailure(item, "Unable to resolve logicalParent PATCH for authorization"));
        continue;
      }
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
