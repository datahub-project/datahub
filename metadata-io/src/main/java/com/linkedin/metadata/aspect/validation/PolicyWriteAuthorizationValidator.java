package com.linkedin.metadata.aspect.validation;

import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
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

/**
 * Requires the Manage Policies privilege for any non-system write to the {@code dataHubPolicyInfo}
 * aspect, regardless of which API the write arrives through.
 *
 * <p>Access policies define who may do what, so editing one is a privilege grant in disguise: an
 * actor who can rewrite a policy's actors or privileges can grant themselves anything. Entity-level
 * authorization keys on {@code (ChangeType, entityType)} at the API layer; this validator makes the
 * same rule hold at the aspect layer as a backstop for any write path that reaches the entity
 * service directly. Bootstrap and other system writes are exempt.
 */
@Setter
@Getter
@Accessors(chain = true)
public class PolicyWriteAuthorizationValidator extends AbstractAspectAuthorizationValidator {

  @Nonnull private AspectPluginConfig config;

  @Override
  protected List<AspectValidationException> validateItems(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull List<? extends BatchItem> items,
      @Nonnull Collection<? extends BatchItem> batchItems,
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull AuthorizationSession session) {

    List<AspectValidationException> failures = new ArrayList<>();
    if (items.isEmpty()) {
      return failures;
    }
    // The privilege is type-level (Manage Policies), so one check covers the whole batch.
    if (EntityAspectAuthorizationUtils.isAuthorizedToManagePolicies(session)) {
      return failures;
    }
    for (BatchItem item : items) {
      failures.add(
          authFailure(
              item,
              "Unauthorized to modify access policy "
                  + item.getUrn()
                  + " (requires Manage Policies)"));
    }
    return failures;
  }
}
