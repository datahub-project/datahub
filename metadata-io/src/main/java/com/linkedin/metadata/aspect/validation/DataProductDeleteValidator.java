package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.DATA_PRODUCT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataproduct.DataProductProperties;
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
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.utils.elasticsearch.FilterUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Blocks deleting a Data Product, hard or soft, while it has child Data Products, so no child is
 * left pointing at a deleted parent. Ingestion rollback is exempt because it removes rows in no
 * particular order.
 */
@Setter
@Getter
@Accessors(chain = true)
public class DataProductDeleteValidator extends AspectPayloadValidator {

  static final String PARENT_DATA_PRODUCT_FIELD = "parentDataProduct";

  @Nonnull private AspectPluginConfig config;

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
      // Deleting the key aspect deletes the entity; deleting any other aspect leaves it in place.
      if (ChangeType.DELETE.equals(item.getChangeType())
          && DATA_PRODUCT_KEY_ASPECT_NAME.equals(item.getAspectName())) {
        checkNoChildren(item, operationContext, retrieverContext, exceptions);
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
        checkNoChildren(item, operationContext, retrieverContext, exceptions);
      }
    }
    return exceptions.streamAllExceptions();
  }

  private static void checkNoChildren(
      @Nonnull BatchItem item,
      @Nonnull OperationFingerprint operationContext,
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull ValidationExceptionCollection exceptions) {
    final Urn parent = item.getUrn();
    // Search leaves out soft-deleted children, so they do not block their parent. If the parent
    // is later hard-deleted, reference cleanup clears their pointer. Two hits are enough to
    // discount a legacy product stored as its own parent and still see a real child.
    ScrollResult result =
        retrieverContext
            .getSearchRetriever()
            .scroll(
                List.of(DATA_PRODUCT_ENTITY_NAME),
                FilterUtils.createValuesFilter(
                    PARENT_DATA_PRODUCT_FIELD, List.of(parent.toString())),
                null,
                2,
                new ArrayList<>(),
                SearchRetriever.RETRIEVER_SEARCH_FLAGS_NO_CACHE_ALL_VERSIONS);
    if (result == null || result.getEntities() == null) {
      return;
    }
    boolean hasChildren =
        result.getEntities().stream()
            .map(SearchEntity::getEntity)
            .anyMatch(child -> !child.equals(parent));
    if (!hasChildren) {
      return;
    }
    // The UI shows this after "Failed to delete:", so it reads as a sentence.
    exceptions.addException(
        AspectValidationException.forItem(
            item,
            String.format(
                "%s has child Data Products. Delete them or move them to another parent first.",
                displayName(parent, operationContext, retrieverContext))));
  }

  private static String displayName(
      @Nonnull Urn urn,
      @Nonnull OperationFingerprint operationContext,
      @Nonnull RetrieverContext retrieverContext) {
    Aspect aspect =
        retrieverContext
            .getAspectRetriever()
            .getLatestAspectObject(operationContext, urn, DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
    if (aspect != null) {
      DataProductProperties properties = new DataProductProperties(aspect.data());
      if (properties.hasName() && !properties.getName().isBlank()) {
        return "\"" + properties.getName() + "\"";
      }
    }
    return "\"" + urn.getId() + "\"";
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
    if (item.getPreviousRecordTemplate() == null) {
      return null;
    }
    return item.getPreviousAspect(Status.class);
  }

  private static boolean isRemoved(@Nullable Status status) {
    return status != null && status.isRemoved();
  }
}
