package com.linkedin.metadata.entity.versioning.validation;

import static com.linkedin.metadata.Constants.VERSION_SET_PROPERTIES_ASPECT_NAME;

import com.datahub.util.RecordUtils;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.versionset.VersionSetProperties;
import java.util.Collection;
import java.util.Optional;
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
public class VersionSetPropertiesValidator extends AspectPayloadValidator {

  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    return validatePropertiesUpserts(
        mcpItems.stream()
            .filter(i -> VERSION_SET_PROPERTIES_ASPECT_NAME.equals(i.getAspectName()))
            .collect(Collectors.toList()),
        retrieverContext);
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  @VisibleForTesting
  public static Stream<AspectValidationException> validatePropertiesUpserts(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    for (BatchItem mcpItem : mcpItems) {
      VersionSetProperties versionSetProperties = mcpItem.getAspect(VersionSetProperties.class);
      Optional<Aspect> aspect =
          Optional.ofNullable(
              retrieverContext
                  .getAspectRetriever()
                  .getLatestAspectObject(mcpItem.getUrn(), VERSION_SET_PROPERTIES_ASPECT_NAME));
      if (aspect.isPresent()) {
        VersionSetProperties previousVersionSetProperties =
            RecordUtils.toRecordTemplate(VersionSetProperties.class, aspect.get().data());
        if (!previousVersionSetProperties
            .getVersioningScheme()
            .equals(versionSetProperties.getVersioningScheme())) {
          exceptions.addException(
              mcpItem,
              "Versioning Scheme cannot change. Expected Scheme: "
                  + previousVersionSetProperties.getVersioningScheme()
                  + " Provided Scheme: "
                  + versionSetProperties.getVersioningScheme());
        }
      }
    }
    return exceptions.streamAllExceptions();
  }
}
