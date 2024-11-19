package com.linkedin.metadata.entity.versioning.validation;

import static com.linkedin.metadata.Constants.VERSION_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.VERSION_SET_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.VERSION_SET_PROPERTIES_ASPECT_NAME;

import com.datahub.util.RecordUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.VersionProperties;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.key.VersionSetKey;
import com.linkedin.versionset.VersionSetProperties;
import com.linkedin.versionset.VersioningScheme;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

@Setter
@Getter
@Slf4j
@Accessors(chain = true)
public class VersionPropertiesValidator extends AspectPayloadValidator {

  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    return validatePropertiesUpserts(
        mcpItems.stream()
            .filter(i -> VERSION_PROPERTIES_ASPECT_NAME.equals(i.getAspectName()))
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
      VersionProperties versionProperties = mcpItem.getAspect(VersionProperties.class);
      // Validate Version Set entity type
      Urn versionSetUrn = versionProperties.getVersionSet();
      Map<String, Aspect> aspects =
          retrieverContext
              .getAspectRetriever()
              .getLatestAspectObjects(
                  Collections.singleton(versionSetUrn),
                  ImmutableSet.of(VERSION_SET_KEY_ASPECT_NAME, VERSION_SET_PROPERTIES_ASPECT_NAME))
              .get(versionSetUrn);
      if (aspects == null || aspects.isEmpty()) {
        exceptions.addException(mcpItem, "Version Set specified does not exist: " + versionSetUrn);
        continue;
      }
      Optional<Aspect> keyAspect = Optional.ofNullable(aspects.get(VERSION_SET_KEY_ASPECT_NAME));
      if (keyAspect.isPresent()) {
        VersionSetKey versionSetKey =
            RecordUtils.toRecordTemplate(VersionSetKey.class, keyAspect.get().data());
        if (!mcpItem.getEntitySpec().getName().equals(versionSetKey.getEntityType())) {
          exceptions.addException(
              mcpItem,
              "Version Set specified entity type does not match, expected type: "
                  + versionSetKey.getEntityType());
        }

        // Validate sort ID scheme
        String sortId = versionProperties.getSortId();
        Optional<Aspect> versionSetPropertiesAspect =
            Optional.ofNullable(aspects.get(VERSION_SET_PROPERTIES_ASPECT_NAME));
        if (versionSetPropertiesAspect.isPresent()) {
          VersionSetProperties versionSetProperties =
              RecordUtils.toRecordTemplate(
                  VersionSetProperties.class, versionSetPropertiesAspect.get().data());
          VersioningScheme versioningScheme = versionSetProperties.getVersioningScheme();
          switch (versioningScheme) {
            case ALPHANUMERIC_GENERATED_BY_DATAHUB:
              validateDataHubGeneratedScheme(sortId, exceptions, mcpItem);
              break;
            default:
              exceptions.addException(mcpItem, "Unsupported schem type: " + versioningScheme);
          }
        } else {
          exceptions.addException(
              mcpItem, "No properties specified for Version Set: " + versionSetUrn);
        }
      } else {
        exceptions.addException(mcpItem, "Version Set specified does not exist: " + versionSetUrn);
      }

      // Validate isLatest not set
      if (versionProperties.hasIsLatest()) {
        exceptions.addException(
            mcpItem, "IsLatest should not be specified, this is a computed field.");
      }
    }
    return exceptions.streamAllExceptions();
  }

  private static void validateDataHubGeneratedScheme(
      String sortId, ValidationExceptionCollection exceptions, BatchItem mcpItem) {
    if (!(sortId.length() == 8
        && StringUtils.isAllUpperCase(sortId)
        && StringUtils.isAlpha(sortId))) {
      exceptions.addException(
          mcpItem,
          "Invalid sortID for Versioning Scheme. ID: "
              + sortId
              + " Scheme: "
              + VersioningScheme.ALPHANUMERIC_GENERATED_BY_DATAHUB);
    }
  }
}
