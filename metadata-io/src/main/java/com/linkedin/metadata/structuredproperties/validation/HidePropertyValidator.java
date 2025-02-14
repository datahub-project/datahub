package com.linkedin.metadata.structuredproperties.validation;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_SETTINGS_ASPECT_NAME;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.structured.StructuredPropertySettings;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Setter
@Getter
@Slf4j
@Accessors(chain = true)
public class HidePropertyValidator extends AspectPayloadValidator {

  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    return validateSettingsUpserts(
        mcpItems.stream()
            .filter(i -> STRUCTURED_PROPERTY_SETTINGS_ASPECT_NAME.equals(i.getAspectName()))
            .collect(Collectors.toList()));
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  @VisibleForTesting
  public static Stream<AspectValidationException> validateSettingsUpserts(
      @Nonnull Collection<? extends BatchItem> mcpItems) {
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    for (BatchItem mcpItem : mcpItems) {
      StructuredPropertySettings structuredPropertySettings =
          mcpItem.getAspect(StructuredPropertySettings.class);
      boolean isValid =
          StructuredPropertyUtils.validatePropertySettings(structuredPropertySettings, false);
      if (!isValid) {
        exceptions.addException(mcpItem, StructuredPropertyUtils.INVALID_SETTINGS_MESSAGE);
      }
    }
    return exceptions.streamAllExceptions();
  }
}
