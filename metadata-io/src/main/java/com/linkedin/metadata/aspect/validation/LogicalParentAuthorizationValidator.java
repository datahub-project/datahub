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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    Map<Urn, Set<Urn>> urnsRequiringEditByChild = new HashMap<>();
    for (BatchItem item : items) {
      Set<Urn> urnsToCheck = new HashSet<>();
      urnsToCheck.add(item.getUrn());
      LogicalParent proposed = item.getAspect(LogicalParent.class);
      if (proposed != null && proposed.hasParent() && proposed.getParent().hasDestinationUrn()) {
        urnsToCheck.add(proposed.getParent().getDestinationUrn());
      }
      urnsRequiringEditByChild.put(item.getUrn(), urnsToCheck);
    }

    Set<Urn> unauthorized =
        EntityAspectAuthorizationUtils.filterUnauthorizedToEditLogicalParent(
            session, urnsRequiringEditByChild);

    List<AspectValidationException> failures = new ArrayList<>();
    for (BatchItem item : items) {
      if (unauthorized.contains(item.getUrn())) {
        failures.add(
            authFailure(item, "Unauthorized to modify logicalParent on entity: " + item.getUrn()));
      }
    }
    return failures;
  }
}
