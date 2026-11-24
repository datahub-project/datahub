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
  private static final int CHILD_SEARCH_PAGE_SIZE = 10;

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
    // Search leaves out soft-deleted children, so they do not block their parent. Search can also
    // still list a child whose pointer reference cleanup already cleared, so each hit is confirmed
    // against its own aspect. Search does not rank live pointers first, so a page of stale hits
    // is not the end of the scroll.
    String scrollId = null;
    while (true) {
      ScrollResult result =
          retrieverContext
              .getSearchRetriever()
              .scroll(
                  List.of(DATA_PRODUCT_ENTITY_NAME),
                  FilterUtils.createValuesFilter(
                      PARENT_DATA_PRODUCT_FIELD, List.of(parent.toString())),
                  scrollId,
                  CHILD_SEARCH_PAGE_SIZE,
                  new ArrayList<>(),
                  SearchRetriever.RETRIEVER_SEARCH_FLAGS_NO_CACHE_ALL_VERSIONS);
      if (result == null || result.getEntities() == null || result.getEntities().isEmpty()) {
        return;
      }
      boolean hasChildren =
          result.getEntities().stream()
              .map(SearchEntity::getEntity)
              .filter(child -> !child.equals(parent))
              .anyMatch(child -> pointsAtParent(child, parent, operationContext, retrieverContext));
      if (hasChildren) {
        break;
      }
      // A scroll that repeats its id would otherwise walk the same page forever.
      String nextScrollId = result.getScrollId();
      if (nextScrollId == null || nextScrollId.equals(scrollId)) {
        return;
      }
      scrollId = nextScrollId;
    }
    // The UI shows this after "Failed to delete:", so it reads as a sentence.
    exceptions.addException(
        AspectValidationException.forItem(
            item,
            String.format(
                "%s has child Data Products. Delete them or move them to another parent first.",
                displayName(parent, operationContext, retrieverContext))));
  }

  /**
   * True when the child's own properties still name {@code parent}. Search lags behind writes, so a
   * hit alone is not proof: reference cleanup removes the pointer in the graph and the aspect
   * before the key delete, while the search document may still show the old parent.
   */
  private static boolean pointsAtParent(
      @Nonnull Urn child,
      @Nonnull Urn parent,
      @Nonnull OperationFingerprint operationContext,
      @Nonnull RetrieverContext retrieverContext) {
    Aspect aspect =
        retrieverContext
            .getAspectRetriever()
            .getLatestAspectObject(operationContext, child, DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
    if (aspect == null) {
      return false;
    }
    DataProductProperties properties = new DataProductProperties(aspect.data());
    return properties.hasParentDataProduct() && parent.equals(properties.getParentDataProduct());
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
