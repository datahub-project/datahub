package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.ALIASES_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Aliases;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.utils.AliasesUtils;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Keeps the system-computed {@code lowercasedUrn} field trustworthy: when set, it must equal the
 * lowercased form of the entity's own URN. The side effect always writes the correct value, so any
 * direct client write that disagrees is rejected — the field cannot be spoofed into a wrong
 * resolution.
 */
@Slf4j
@Setter
@Getter
@Accessors(chain = true)
public class AliasesValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    for (BatchItem item : mcpItems) {
      if (!ALIASES_ASPECT_NAME.equals(item.getAspectName())) {
        continue;
      }
      Aliases aspect = item.getAspect(Aliases.class);
      if (aspect == null || !aspect.hasLowercasedUrn()) {
        continue;
      }
      try {
        Urn expected = AliasesUtils.lowercaseDatasetUrn(item.getUrn());
        if (!expected.equals(aspect.getLowercasedUrn())) {
          exceptions.addException(
              AspectValidationException.forItem(
                  item,
                  String.format(
                      "lowercasedUrn is system-computed and cannot be set directly; expected '%s'",
                      expected)));
        }
      } catch (URISyntaxException e) {
        exceptions.addException(
            AspectValidationException.forItem(
                item, "Unable to validate lowercasedUrn for " + item.getUrn(), e));
      }
    }

    return exceptions.streamAllExceptions();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }
}
