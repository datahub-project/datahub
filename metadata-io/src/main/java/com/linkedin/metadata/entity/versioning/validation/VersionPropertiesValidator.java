package com.linkedin.metadata.entity.versioning.validation;

import static com.linkedin.metadata.Constants.VERSION_LABEL_FIELD_NAME;
import static com.linkedin.metadata.Constants.VERSION_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.VERSION_SET_FIELD_NAME;
import static com.linkedin.metadata.Constants.VERSION_SET_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.VERSION_SET_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.VERSION_SORT_ID_FIELD_NAME;

import com.datahub.util.RecordUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.VersionProperties;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.key.VersionSetKey;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.versionset.VersionSetProperties;
import com.linkedin.versionset.VersioningScheme;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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

  private static final Set<ChangeType> SHOULD_VALIDATE_PROPOSED =
      ImmutableSet.of(ChangeType.UPDATE, ChangeType.UPSERT, ChangeType.CREATE);

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    return validatePropertiesProposals(
        mcpItems.stream()
            .filter(mcpItem -> VERSION_PROPERTIES_ASPECT_NAME.equals(mcpItem.getAspectName()))
            .filter(mcpItem -> SHOULD_VALIDATE_PROPOSED.contains(mcpItem.getChangeType()))
            .collect(Collectors.toList()));
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    return validatePropertiesUpserts(
        changeMCPs.stream()
            .filter(changeMCP -> VERSION_PROPERTIES_ASPECT_NAME.equals(changeMCP.getAspectName()))
            .collect(Collectors.toList()),
        retrieverContext);
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
        // Validate sort id matches scheme if version set properties exist
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
              exceptions.addException(mcpItem, "Unsupported scheme type: " + versioningScheme);
          }
        }
      } else {
        exceptions.addException(mcpItem, "Version Set specified does not exist: " + versionSetUrn);
      }

      // Best effort validate on uniqueness for sort ID and version label, search has potential
      // timing gap
      SearchRetriever searchRetriever = retrieverContext.getSearchRetriever();
      Filter versionSetAndSortIdOrVersionLabelMatch =
          QueryUtils.newDisjunctiveFilter(
              CriterionUtils.buildConjunctiveCriterion(
                  CriterionUtils.buildCriterion(
                      VERSION_SET_FIELD_NAME, Condition.EQUAL, versionSetUrn.toString()),
                  CriterionUtils.buildCriterion(
                      VERSION_SORT_ID_FIELD_NAME, Condition.EQUAL, versionProperties.getSortId())),
              CriterionUtils.buildConjunctiveCriterion(
                  CriterionUtils.buildCriterion(
                      VERSION_SET_FIELD_NAME, Condition.EQUAL, versionSetUrn.toString()),
                  CriterionUtils.buildCriterion(
                      VERSION_LABEL_FIELD_NAME,
                      Condition.EQUAL,
                      versionProperties.getVersion().getVersionTag())));
      ScrollResult scrollResult =
          searchRetriever.scroll(
              ImmutableList.of(mcpItem.getEntitySpec().getName()),
              versionSetAndSortIdOrVersionLabelMatch,
              null,
              1,
              ImmutableList.of(
                  new SortCriterion()
                      .setField(VERSION_SORT_ID_FIELD_NAME)
                      .setOrder(SortOrder.DESCENDING)),
              SearchRetriever.RETRIEVER_SEARCH_FLAGS_NO_CACHE_ALL_VERSIONS);
      List<SearchEntity> matchedEntities =
          scrollResult.getEntities().stream()
              .filter(entity -> !mcpItem.getUrn().equals(entity.getEntity()))
              .collect(Collectors.toList());
      if (!matchedEntities.isEmpty()) {
        exceptions.addException(
            mcpItem,
            "Sort Id and Version label must be unique: sort ID: "
                + versionProperties.getSortId()
                + " version label: "
                + versionProperties.getVersion().getVersionTag());
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

  @VisibleForTesting
  public static Stream<AspectValidationException> validatePropertiesProposals(
      @Nonnull Collection<? extends BatchItem> mcpItems) {
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    for (BatchItem mcpItem : mcpItems) {
      if (mcpItem instanceof PatchItemImpl) {
        throw new IllegalStateException("Patch item must have change type of PATCH.");
      }
      VersionProperties versionProperties = mcpItem.getAspect(VersionProperties.class);
      // Validate isLatest not set
      if (versionProperties.hasIsLatest()) {
        exceptions.addException(
            mcpItem, "IsLatest should not be specified, this is a computed field.");
      }
    }
    return exceptions.streamAllExceptions();
  }
}
