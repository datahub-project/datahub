package com.linkedin.metadata.structuredproperties.validation;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_ENTITY_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_SETTINGS_ASPECT_NAME;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.datahub.util.RecordUtils;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.entity.GenericScrollIterator;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.structured.StructuredPropertySettings;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

@Setter
@Getter
@Slf4j
@Accessors(chain = true)
public class ShowPropertyAsBadgeValidator extends AspectPayloadValidator {

  @Nonnull private AspectPluginConfig config;

  private static final String SHOW_ASSET_AS_BADGE_FIELD = "showAsAssetBadge";

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    return validateSettingsUpserts(
        mcpItems.stream()
            .filter(i -> STRUCTURED_PROPERTY_SETTINGS_ASPECT_NAME.equals(i.getAspectName()))
            .collect(Collectors.toList()),
        retrieverContext);
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  @VisibleForTesting
  public static Stream<AspectValidationException> validateSettingsUpserts(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    for (BatchItem mcpItem : mcpItems) {
      StructuredPropertySettings structuredPropertySettings =
          mcpItem.getAspect(StructuredPropertySettings.class);
      if (structuredPropertySettings.isShowAsAssetBadge()) {
        // Search for any structured properties that have showAsAssetBadge set, should only ever be
        // one at most.
        GenericScrollIterator scrollIterator =
            GenericScrollIterator.builder()
                .searchRetriever(retrieverContext.getSearchRetriever())
                .count(10) // Get first 10, should only ever be one, but this gives us more info if
                // we're in a bad state
                .filter(getFilter())
                .entities(Collections.singletonList(STRUCTURED_PROPERTY_ENTITY_NAME))
                .build();
        // Only need to get first set, if there are more then will have to resolve bad state
        ScrollResult scrollResult = scrollIterator.next();
        if (CollectionUtils.isNotEmpty(scrollResult.getEntities())) {
          if (scrollResult.getEntities().size() > 1) {
            // If it's greater than one, don't bother querying DB since we for sure are in a bad
            // state
            exceptions.addException(
                mcpItem,
                StructuredPropertyUtils.ONLY_ONE_BADGE
                    + scrollResult.getEntities().stream()
                        .map(SearchEntity::getEntity)
                        .collect(Collectors.toList()));
          } else {
            // If there is just one, verify against DB to make sure we're not hitting a timing issue
            // with eventual consistency
            AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();
            Optional<Aspect> propertySettings =
                Optional.ofNullable(
                    aspectRetriever.getLatestAspectObject(
                        scrollResult.getEntities().get(0).getEntity(),
                        STRUCTURED_PROPERTY_SETTINGS_ASPECT_NAME));
            if (propertySettings.isPresent()) {
              StructuredPropertySettings dbBadgeSettings =
                  RecordUtils.toRecordTemplate(
                      StructuredPropertySettings.class, propertySettings.get().data());
              if (dbBadgeSettings.isShowAsAssetBadge()) {
                exceptions.addException(
                    mcpItem,
                    StructuredPropertyUtils.ONLY_ONE_BADGE
                        + scrollResult.getEntities().stream()
                            .map(SearchEntity::getEntity)
                            .collect(Collectors.toList()));
              }
            }
          }
        }
      }
    }
    return exceptions.streamAllExceptions();
  }

  private static Filter getFilter() {
    Filter propertyFilter = new Filter();
    final ConjunctiveCriterionArray disjunction = new ConjunctiveCriterionArray();
    final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
    final CriterionArray andCriterion = new CriterionArray();

    final Criterion propertyExistsCriterion =
        buildCriterion(SHOW_ASSET_AS_BADGE_FIELD, Condition.EQUAL, "true");

    andCriterion.add(propertyExistsCriterion);
    conjunction.setAnd(andCriterion);
    disjunction.add(conjunction);
    propertyFilter.setOr(disjunction);

    return propertyFilter;
  }
}
