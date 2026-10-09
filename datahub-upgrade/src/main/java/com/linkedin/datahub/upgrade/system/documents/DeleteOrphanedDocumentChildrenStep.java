package com.linkedin.datahub.upgrade.system.documents;

import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DOCUMENT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOCUMENT_INFO_ASPECT_NAME;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.knowledge.DocumentInfo;
import com.linkedin.knowledge.ParentDocument;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.service.DocumentDeleteLimitException;
import com.linkedin.metadata.service.DocumentDeleteResult;
import com.linkedin.metadata.service.DocumentService;
import com.linkedin.metadata.service.SearchIndexMode;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Soft-deletes live documents whose parent document is soft-deleted or gone. The candidate list is
 * collected before any delete so removals cannot disturb scroll pagination. Each orphan is removed
 * through {@link DocumentService#deleteDocument}, which also removes that document's live
 * descendants.
 *
 * <p>A tree over the delete cap is logged and left untouched. Other orphans in the run are still
 * processed. The marker records how many documents were removed and which over-cap roots were left,
 * including when the step result is failed, so the step still runs once. Reduce those trees under
 * the cap, then set {@code systemUpdate.deleteOrphanedDocumentChildren.reprocess.enabled} to retry
 * them on the next system update.
 */
@Slf4j
public class DeleteOrphanedDocumentChildrenStep implements UpgradeStep {

  static final String UPGRADE_ID = "delete-orphaned-document-children-v1";

  private static final int SKIPPED_ROOT_SAMPLE_LIMIT = 20;

  @Nonnull
  private static final Urn UPGRADE_ID_URN =
      Objects.requireNonNull(BootstrapStep.getUpgradeUrn(UPGRADE_ID));

  private static final String PARENT_DOCUMENT_FIELD = "parentDocument";

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final SearchService searchService;
  private final DocumentService documentService;
  private final boolean reprocessEnabled;
  private final Integer batchSize;

  public DeleteOrphanedDocumentChildrenStep(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      DocumentService documentService,
      boolean reprocessEnabled,
      Integer batchSize) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.searchService = searchService;
    this.documentService = documentService;
    this.reprocessEnabled = reprocessEnabled;
    this.batchSize = batchSize;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      final OperationContext runContext = runContext(Objects.requireNonNull(context));
      final List<Urn> candidates = scrollDocumentsWithParent(runContext);
      final Set<Urn> runVisited = new HashSet<>();
      int deletedRootCount = 0;
      int deletedUrnCount = 0;
      int overCapRootCount = 0;
      final List<String> skippedRoots = new ArrayList<>();
      final int chunkSize =
          batchSize == null || batchSize < 1 ? DocumentDeleteResult.SCROLL_PAGE_SIZE : batchSize;

      for (int start = 0; start < candidates.size(); start += chunkSize) {
        final int end = Math.min(start + chunkSize, candidates.size());
        final List<Urn> pending = new ArrayList<>();
        for (Urn candidate : candidates.subList(start, end)) {
          final Urn urn = Objects.requireNonNull(candidate);
          if (!runVisited.contains(urn)) {
            pending.add(urn);
          }
        }
        if (pending.isEmpty()) {
          continue;
        }
        final Map<Urn, Urn> parentByChild = storedParents(runContext, pending);
        final Set<Urn> liveParents =
            parentByChild.isEmpty()
                ? Set.of()
                : Objects.requireNonNull(
                    entityService.exists(runContext, new HashSet<>(parentByChild.values()), false));
        for (Urn urn : pending) {
          if (runVisited.contains(urn)) {
            continue;
          }
          final Urn parent = parentByChild.get(urn);
          // A missing stored parent was already logged. A live parent keeps the document.
          // includeSoftDeleted=false treats a soft-deleted parent and a missing parent URN as gone.
          if (parent == null || liveParents.contains(parent)) {
            continue;
          }
          try {
            final DocumentDeleteResult deleted =
                documentService.deleteDocument(runContext, urn, SearchIndexMode.SYNC);
            runVisited.addAll(deleted.urns());
            deletedRootCount++;
            deletedUrnCount += deleted.urns().size();
          } catch (DocumentDeleteLimitException e) {
            overCapRootCount++;
            if (skippedRoots.size() < SKIPPED_ROOT_SAMPLE_LIMIT) {
              skippedRoots.add(urn.toString());
            }
            log.warn(
                "Document tree rooted at {} exceeds the delete cap and was left in place. Delete"
                    + " nested documents until it is under the cap, then set"
                    + " systemUpdate.deleteOrphanedDocumentChildren.reprocess.enabled so the next"
                    + " system update retries {}",
                urn,
                id(),
                e);
          } catch (RuntimeException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to delete orphaned document %s", urn), e);
          }
        }
      }

      final DataHubUpgradeState state =
          overCapRootCount > 0 ? DataHubUpgradeState.FAILED : DataHubUpgradeState.SUCCEEDED;
      log.info(
          "{} deleted {} document roots ({} documents) and left {} trees over the delete cap",
          id(),
          deletedRootCount,
          deletedUrnCount,
          overCapRootCount);
      final Map<String, String> result = new HashMap<>();
      result.put("deletedRootCount", String.valueOf(deletedRootCount));
      result.put("deletedUrnCount", String.valueOf(deletedUrnCount));
      result.put("overCapRootCount", String.valueOf(overCapRootCount));
      result.put("skippedRoots", String.join(",", skippedRoots));
      BootstrapStep.setUpgradeResult(runContext, UPGRADE_ID_URN, entityService, state, result);
      return new DefaultUpgradeStepResult(id(), state);
    };
  }

  @Nonnull
  private OperationContext runContext(@Nonnull UpgradeContext context) {
    final OperationContext fromUpgrade = context.opContext();
    return Objects.requireNonNull(fromUpgrade != null ? fromUpgrade : opContext);
  }

  @Nonnull
  private Map<Urn, Urn> storedParents(
      @Nonnull OperationContext runContext, @Nonnull List<Urn> documents) {
    final Map<Urn, List<RecordTemplate>> aspects =
        entityService.getLatestAspects(
            runContext,
            new HashSet<>(documents),
            Objects.requireNonNull(Set.of(DOCUMENT_INFO_ASPECT_NAME)),
            false);
    final Map<Urn, List<RecordTemplate>> stored = aspects == null ? Map.of() : aspects;
    final Map<Urn, Urn> parentByChild = new HashMap<>();
    for (Urn document : documents) {
      final Urn parent = parentUrn(stored.get(document));
      if (parent == null) {
        log.warn("Skipping document {} with no stored parent", document);
        continue;
      }
      parentByChild.put(document, parent);
    }
    return parentByChild;
  }

  @Nullable
  private static Urn parentUrn(@Nullable List<RecordTemplate> aspects) {
    if (aspects == null) {
      return null;
    }
    for (RecordTemplate template : aspects) {
      if (template == null) {
        continue;
      }
      final DocumentInfo info =
          template instanceof DocumentInfo
              ? (DocumentInfo) template
              : new DocumentInfo(template.data());
      final ParentDocument parent = info.getParentDocument();
      if (parent == null || !parent.hasDocument()) {
        continue;
      }
      final Urn parentUrn = parent.getDocument();
      if (parentUrn != null) {
        return parentUrn;
      }
    }
    return null;
  }

  /**
   * Every live document that has a parent, fully collected before any mutation. Same search flags
   * as {@link DocumentService#deleteDocument}: drafts and non-global documents are included,
   * removed documents are not.
   */
  @Nonnull
  private List<Urn> scrollDocumentsWithParent(@Nonnull OperationContext runContext) {
    final List<Urn> urns = new ArrayList<>();
    String scrollId = null;
    final OperationContext scrollContext = DocumentService.withLiveSubtreeSearchFlags(runContext);
    do {
      final ScrollResult scrollResult =
          searchService.scrollAcrossEntities(
              scrollContext,
              Objects.requireNonNull(ImmutableList.of(DOCUMENT_ENTITY_NAME)),
              "*",
              parentDocumentExistsFilter(),
              null,
              scrollId,
              null,
              batchSize,
              null);
      if (scrollResult == null) {
        throw new IllegalStateException("Scroll for documents with a parent returned no result");
      }
      final String nextScrollId = scrollResult.getScrollId();
      final List<SearchEntity> entities = scrollResult.getEntities();
      if (entities == null || entities.isEmpty()) {
        if (nextScrollId != null) {
          throw new IllegalStateException(
              "Scroll for documents with a parent returned an empty page with scroll id "
                  + nextScrollId);
        }
        break;
      }
      for (SearchEntity entity : entities) {
        urns.add(Objects.requireNonNull(entity).getEntity());
      }
      scrollId = nextScrollId;
    } while (scrollId != null);
    return urns;
  }

  @Nonnull
  private static Filter parentDocumentExistsFilter() {
    final ConjunctiveCriterion conjunction =
        new ConjunctiveCriterion()
            .setAnd(
                new CriterionArray(
                    ImmutableList.of(CriterionUtils.buildExistsCriterion(PARENT_DOCUMENT_FIELD))));
    return Objects.requireNonNull(
        new Filter().setOr(new ConjunctiveCriterionArray(ImmutableList.of(conjunction))));
  }

  @Override
  public String id() {
    return UPGRADE_ID;
  }

  @Override
  public boolean isOptional() {
    return true;
  }

  @Override
  public boolean skip(UpgradeContext context) {
    if (reprocessEnabled) {
      return false;
    }
    final boolean previouslyRun =
        entityService.exists(
            runContext(Objects.requireNonNull(context)),
            UPGRADE_ID_URN,
            DATA_HUB_UPGRADE_RESULT_ASPECT_NAME,
            true);
    if (previouslyRun) {
      log.info("{} was already run. Skipping.", id());
    }
    return previouslyRun;
  }
}
