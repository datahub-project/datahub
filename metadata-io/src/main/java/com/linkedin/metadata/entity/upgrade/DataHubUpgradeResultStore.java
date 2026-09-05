package com.linkedin.metadata.entity.upgrade;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Read/write access to the {@code dataHubUpgradeResult} aspect, abstracted over the two ways a
 * component can reach primary storage.
 *
 * <p>GMS holds a local {@code EntityService} and uses {@code EntityServiceUpgradeResultStore}. The
 * standalone MAE consumer does not: it runs {@code entityClient.impl=restli} and has no datasource,
 * so it uses {@link EntityClientUpgradeResultStore} and reaches storage through GMS. See {@code
 * UpdateIndicesServiceFactory#searchIndicesServiceNonGMS} for the same split.
 */
public interface DataHubUpgradeResultStore {

  /**
   * Latest {@code dataHubUpgradeResult} for the given upgrade, or {@code null} when absent. The
   * returned aspect carries {@code systemMetadata.version}, which {@link
   * DataHubUpgradeResultConditionalPersist} needs for its {@code If-Version-Match} precondition, so
   * implementations must not serve it from a cache.
   */
  @Nullable
  EnvelopedAspect readLatest(OperationContext opContext, @Nonnull final Urn upgradeIdUrn)
      throws Exception;

  void ingest(OperationContext opContext, @Nonnull final MetadataChangeProposal proposal)
      throws Exception;
}
