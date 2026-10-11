package com.linkedin.metadata.entity.upgrade;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;

/** In-process store for callers that already hold an {@link EntityService}, such as GMS. */
@RequiredArgsConstructor
class EntityServiceUpgradeResultStore implements DataHubUpgradeResultStore {

  @Nonnull private final EntityService<?> entityService;

  @Override
  @Nullable
  public EnvelopedAspect readLatest(OperationContext opContext, @Nonnull final Urn upgradeIdUrn)
      throws Exception {
    return entityService.getLatestEnvelopedAspect(
        opContext,
        Constants.DATA_HUB_UPGRADE_ENTITY_NAME,
        upgradeIdUrn,
        Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME);
  }

  @Override
  public void ingest(OperationContext opContext, @Nonnull final MetadataChangeProposal proposal) {
    entityService.ingestProposal(
        opContext, proposal, AuditStampUtils.createDefaultAuditStamp(), false);
  }
}
