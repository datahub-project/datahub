package com.linkedin.datahub.upgrade.system.documents;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.service.DocumentService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * One-time soft-delete of live documents whose parent is soft-deleted or missing. See {@link
 * DeleteOrphanedDocumentChildrenStep}.
 */
public class DeleteOrphanedDocumentChildren implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> steps;

  public DeleteOrphanedDocumentChildren(
      @Nonnull OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      DocumentService documentService,
      boolean enabled,
      boolean reprocessEnabled,
      Integer batchSize) {
    if (enabled) {
      steps =
          ImmutableList.of(
              new DeleteOrphanedDocumentChildrenStep(
                  opContext,
                  entityService,
                  searchService,
                  documentService,
                  reprocessEnabled,
                  batchSize));
    } else {
      steps = ImmutableList.of();
    }
  }

  @Override
  public String id() {
    return this.getClass().getName();
  }

  @Override
  public List<UpgradeStep> steps() {
    return steps;
  }
}
