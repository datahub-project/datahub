package com.linkedin.metadata.aspect.validation;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import java.util.Collection;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Validator to ensure that system users (i. e: Those can come natively with DataHub) are not
 * deleted. *
 */
@Slf4j
@Setter
@Getter
@Accessors(chain = true)
public class UserDeleteValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    mcpItems.forEach(
        i -> {
          final Urn entityUrn = i.getUrn();

          final Aspect aspect =
              retrieverContext
                  .getAspectRetriever()
                  .getLatestAspectObject(entityUrn, Constants.CORP_USER_INFO_ASPECT_NAME);

          // Skip users for which we don't have corp user information.
          // Should not happen
          if (aspect == null) {
            log.warn(
                "CorpUser {} does not have have a CorpUserInfo aspect, ignoring...", entityUrn);
            return;
          }

          final CorpUserInfo corpUserInfo = new CorpUserInfo(aspect.data());
          if (corpUserInfo.isSystem()) {
            exceptions.addException(
                AspectValidationException.forItem(i, "System users can not be deleted"));
          }
        });

    return exceptions.streamAllExceptions();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }
}
