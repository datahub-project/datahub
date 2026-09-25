package com.linkedin.metadata.utils;

import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.UI_SOURCE;

import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Detects metadata writes that should trigger a <em>synchronous</em> Elasticsearch index update
 * during ingest ({@code UpdateIndicesService.handleChangeEvent}), rather than waiting for the MAE
 * consumer. Graph cache invalidation on the UI fast-path uses the same gate.
 *
 * <p>This is unrelated to the {@code async} flag on {@code EntityServiceImpl.ingestProposal}, which
 * only controls SQL commit vs proposal-log write.
 */
public final class SyncSearchIndexUtils {

  private SyncSearchIndexUtils() {}

  /**
   * Whether ingest should run {@code UpdateIndicesService} inline for this write. True when UI
   * preprocess hooks are enabled and the MCP/MCL carries {@code APP_SOURCE=ui} (e.g. GraphQL {@code
   * MutationUtils}, {@code AspectUtils.buildSynchronousMetadataChangeProposal}).
   *
   * <p>Also see {@link com.linkedin.metadata.Constants#SYNC_INDEX_UPDATE_HEADER_NAME} for the
   * header-based equivalent.
   */
  public static boolean requiresSyncSearchIndexUpdate(
      @Nonnull PreProcessHooks preProcessHooks,
      @Nonnull OperationContext opContext,
      @Nullable SystemMetadata systemMetadata) {
    if (!preProcessHooks.isUiEnabled()) {
      return false;
    }
    return isUiSource(systemMetadata);
  }

  private static boolean isUiSource(@Nullable SystemMetadata systemMetadata) {
    return systemMetadata != null
        && systemMetadata.hasProperties()
        && UI_SOURCE.equals(systemMetadata.getProperties().get(APP_SOURCE));
  }
}
