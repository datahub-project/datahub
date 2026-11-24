package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.CUSTOM_FABRIC_TYPE_KEY_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.FabricTypeNames;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import java.util.Collection;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Checks the id of a new customFabricType: custom types must be enabled, and the id must be
 * well-formed and not a built-in {@code FabricType} symbol in any case. The entity key already
 * keeps ids unique.
 */
@Setter
@Getter
@Accessors(chain = true)
public class CustomFabricTypeKeyValidator extends AspectPayloadValidator {

  @Nonnull private AspectPluginConfig config;

  /** {@code featureFlags.customFabricTypesEnabled}, read on every batch. */
  @Nonnull private BooleanSupplier customTypesEnabled;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    for (ChangeMCP item : changeMCPs) {
      // The key is written only when the entity is created; an existing type is never re-checked.
      if (!CUSTOM_FABRIC_TYPE_KEY_ASPECT_NAME.equals(item.getAspectName())
          || item.getPreviousRecordTemplate() != null) {
        continue;
      }
      String id = item.getUrn().getId();
      String message = null;
      if (!customTypesEnabled.getAsBoolean()) {
        message = "Custom environments are not enabled on this server.";
      } else if (FabricTypeNames.collidesWithBuiltIn(id)) {
        message = String.format("\"%s\" is a built-in environment.", id);
      } else if (!FabricTypeNames.isWellFormed(id)) {
        message =
            String.format(
                "\"%s\" is not a valid environment id: use uppercase letters, digits and"
                    + " underscores, starting with a letter, at most 64 characters.",
                id);
      }
      if (message != null) {
        exceptions.addException(AspectValidationException.forItem(item, message));
      }
    }
    return exceptions.streamAllExceptions();
  }
}
