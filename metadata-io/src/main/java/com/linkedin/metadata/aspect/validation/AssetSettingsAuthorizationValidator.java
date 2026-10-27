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
 * Requires {@code EDIT_ENTITY} or {@code MANAGE_ASSET_SUMMARY} on the target asset for any write to
 * its {@code assetSettings} aspect, regardless of which API the write arrives through.
 */
@Setter
@Getter
@Accessors(chain = true)
public class AssetSettingsAuthorizationValidator extends AbstractAspectAuthorizationValidator {

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
      if (!EntityAspectAuthorizationUtils.isAuthorizedToEditAssetSettings(session, item.getUrn())) {
        failures.add(
            authFailure(item, "Unauthorized to modify assetSettings on entity: " + item.getUrn()));
      }
    }
    return failures;
  }
}
