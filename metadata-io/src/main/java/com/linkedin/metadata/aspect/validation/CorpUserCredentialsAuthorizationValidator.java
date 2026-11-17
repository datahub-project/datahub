package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.CORP_USER_CREDENTIALS_ASPECT_NAME;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.PoliciesConfig;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Requires Manage User Credentials for any write to {@code corpUserCredentials}, regardless of
 * which API the write arrives through. A general user-edit privilege is not enough to change
 * native-user secrets.
 */
@Setter
@Getter
@Accessors(chain = true)
public class CorpUserCredentialsAuthorizationValidator
    extends AbstractAspectAuthorizationValidator {

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
      if (!CORP_USER_CREDENTIALS_ASPECT_NAME.equals(item.getAspectName())) {
        continue;
      }
      if (!AuthUtil.isAuthorized(session, PoliciesConfig.MANAGE_USER_CREDENTIALS_PRIVILEGE)) {
        failures.add(
            authFailure(
                item, "Unauthorized to modify corpUserCredentials on entity: " + item.getUrn()));
      }
    }
    return failures;
  }
}
