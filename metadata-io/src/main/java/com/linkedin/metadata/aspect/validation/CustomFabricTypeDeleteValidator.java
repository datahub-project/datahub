package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.CUSTOM_FABRIC_TYPE_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.utils.elasticsearch.FilterUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Blocks deleting a customFabricType, hard or soft, while any entity uses it as its environment.
 * Deprecation is the way to retire a type that is in use. Ingestion rollback is exempt because it
 * removes rows in no particular order.
 *
 * <p>Every searchable environment field is checked by one search, which ORs them together. A field
 * that isn't searchable is not checked (see {@link #setFields}). Search is eventually consistent,
 * so an entity written within the indexing lag can be missed; reads tolerate an unknown
 * environment, so such an entity is never broken.
 */
@Slf4j
@Getter
@Setter
@Accessors(chain = true)
public class CustomFabricTypeDeleteValidator extends AspectPayloadValidator {

  static final int MAX_EXAMPLES = 5;

  @Nonnull private AspectPluginConfig config;

  /** The environment fields to search, from {@link FabricTypeNameFields#discover}. */
  private List<FabricTypeNameFields.Field> fields;

  /**
   * Fields that aren't searchable are not checked, so a type can be deleted while one of them still
   * uses it. Warned about here, at startup, so the gap is visible.
   */
  public CustomFabricTypeDeleteValidator setFields(
      @Nonnull List<FabricTypeNameFields.Field> fields) {
    this.fields = fields;
    fields.stream()
        .filter(field -> field.getSearchFieldName() == null)
        .forEach(
            field ->
                log.warn(
                    "Deleting a customFabricType in use by {}.{}.{} is not blocked: the field is"
                        + " not searchable",
                    field.getEntityName(),
                    field.getAspectName(),
                    field.getFieldName()));
    return this;
  }

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    if (isRollback(operationContext)) {
      return Stream.empty();
    }
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    for (BatchItem item : mcpItems) {
      if (ChangeType.DELETE.equals(item.getChangeType())
          && CUSTOM_FABRIC_TYPE_KEY_ASPECT_NAME.equals(item.getAspectName())) {
        checkNotInUse(item, operationContext, retrieverContext, exceptions);
      }
    }
    return exceptions.streamAllExceptions();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    if (isRollback(operationContext)) {
      return Stream.empty();
    }
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    for (ChangeMCP item : changeMCPs) {
      if (STATUS_ASPECT_NAME.equals(item.getAspectName())
          && isRemoved(item.getAspect(Status.class))
          && !isRemoved(previousStatus(item))) {
        checkNotInUse(item, operationContext, retrieverContext, exceptions);
      }
    }
    return exceptions.streamAllExceptions();
  }

  private void checkNotInUse(
      @Nonnull BatchItem item,
      @Nonnull OperationFingerprint operationContext,
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull ValidationExceptionCollection exceptions) {
    String id = item.getUrn().getId();
    Set<Urn> users = findUsers(id, operationContext, retrieverContext);
    if (users.isEmpty()) {
      return;
    }
    exceptions.addException(
        AspectValidationException.forItem(
            item,
            String.format(
                "Environment \"%s\" is in use, for example by %s. Deprecate it instead, or move"
                    + " those entities to another environment first.",
                id, users.stream().map(Urn::toString).collect(Collectors.joining(", ")))));
  }

  /**
   * Up to {@link #MAX_EXAMPLES} entities whose environment is {@code id}, found by one search: the
   * indexed environment fields are OR-ed together, over every entity type that has one.
   */
  @Nonnull
  Set<Urn> findUsers(
      @Nonnull String id,
      @Nonnull OperationFingerprint operationContext,
      @Nonnull RetrieverContext retrieverContext) {
    List<FabricTypeNameFields.Field> indexed =
        fields.stream()
            .filter(field -> field.getSearchFieldName() != null)
            .collect(Collectors.toList());
    if (indexed.isEmpty()) {
      return new LinkedHashSet<>();
    }

    ConjunctiveCriterionArray alternatives = new ConjunctiveCriterionArray();
    indexed.stream()
        .map(FabricTypeNameFields.Field::getSearchFieldName)
        .distinct()
        .forEach(
            searchField ->
                alternatives.addAll(
                    FilterUtils.createValuesFilter(searchField, List.of(id)).getOr()));
    List<String> entityNames =
        indexed.stream()
            .map(FabricTypeNameFields.Field::getEntityName)
            .distinct()
            .collect(Collectors.toList());

    ScrollResult result =
        retrieverContext
            .getSearchRetriever()
            .scroll(
                entityNames,
                new Filter().setOr(alternatives),
                null,
                MAX_EXAMPLES,
                new ArrayList<>(),
                SearchRetriever.RETRIEVER_SEARCH_FLAGS_NO_CACHE_ALL_VERSIONS_INCLUDE_SOFT_DELETED);
    return entities(result).collect(Collectors.toCollection(LinkedHashSet::new));
  }

  @Nonnull
  private static Stream<Urn> entities(@Nullable ScrollResult result) {
    if (result == null || result.getEntities() == null) {
      return Stream.empty();
    }
    return result.getEntities().stream().map(SearchEntity::getEntity);
  }

  private static boolean isRollback(@Nonnull OperationFingerprint operationContext) {
    if (!(operationContext instanceof OperationContext)) {
      return false;
    }
    OperationContext opContext = (OperationContext) operationContext;
    return opContext.getValidationContext() != null
        && opContext.getValidationContext().isRollbackDeletion();
  }

  @Nullable
  private static Status previousStatus(@Nonnull ChangeMCP item) {
    RecordTemplate previous = item.getPreviousRecordTemplate();
    return previous == null ? null : item.getPreviousAspect(Status.class);
  }

  private static boolean isRemoved(@Nullable Status status) {
    return status != null && status.isRemoved();
  }
}
