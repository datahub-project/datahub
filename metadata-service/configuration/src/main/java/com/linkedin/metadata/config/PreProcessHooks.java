package com.linkedin.metadata.config;

import lombok.Data;

@Data
public class PreProcessHooks {
  /**
   * Synchronous GMS index updates for writes tagged {@code appSource=ui} (GraphQL). Env: {@code
   * PRE_PROCESS_HOOKS_UI_ENABLED}.
   */
  private boolean uiEnabled;

  /**
   * Asynchronous MAE {@code UpdateIndicesHook} processing of the same UI-sourced events. Env:
   * {@code PRE_PROCESS_HOOKS_REPROCESS_ENABLED} (falls back to {@code
   * PRE_PROCESS_HOOKS_UI_ENABLED}).
   */
  private boolean reprocessEnabled;

  /**
   * Same gate as {@code MetadataChangeLogProcessorCondition}: only lowercase {@code true} enables
   * the MCL consumer. Spring boolean conversion would treat {@code TRUE}/{@code on} as enabled and
   * refuse startup in a process that is not actually consuming.
   */
  public static boolean isMclConsumerEnabled(String maeConsumerEnabled, String mclConsumerEnabled) {
    return "true".equals(maeConsumerEnabled) || "true".equals(mclConsumerEnabled);
  }

  /**
   * Rejects the silent-drop configuration from issue 19119: this process is responsible for
   * indexing UI-sourced writes and both paths are off. UI writes land in primary storage, the MAE
   * hook skips {@code appSource=ui}, and search never sees the entity.
   *
   * <p>Call from GMS ({@code EntityServiceFactory}) and from {@code UpdateIndicesHook} (embedded
   * GMS and standalone MAE). Both callers pass whether <em>this</em> process consumes MCLs. Helm
   * should set the same {@code PRE_PROCESS_HOOKS_*} pair on GMS and MAE: {@code uiEnabled} is the
   * GMS GraphQL fast path, {@code reprocessEnabled} is the MAE async path.
   *
   * <p>Kubernetes scale-down sets preprocess off and {@code MAE_CONSUMER_ENABLED=false} on GMS, so
   * neither call fires during system-update. Standalone MAE is scaled to zero in that window.
   *
   * @param hooks preprocess flag pair; {@code null} is treated as both paths disabled
   * @param mclConsumerEnabled true when this process consumes MCLs
   */
  public static void validateWhenConsumingMcl(PreProcessHooks hooks, boolean mclConsumerEnabled) {
    if (!mclConsumerEnabled) {
      return;
    }
    if (hooks != null && (hooks.isUiEnabled() || hooks.isReprocessEnabled())) {
      return;
    }
    throw new IllegalStateException(
        "Invalid search indexing configuration: PRE_PROCESS_HOOKS_UI_ENABLED and "
            + "PRE_PROCESS_HOOKS_REPROCESS_ENABLED are both false while this process is consuming "
            + "MCLs (MAE_CONSUMER_ENABLED or MCL_CONSUMER_ENABLED). UI-sourced writes "
            + "(appSource=ui) are stored but never indexed. Set PRE_PROCESS_HOOKS_UI_ENABLED=true "
            + "for the synchronous GMS path, or PRE_PROCESS_HOOKS_REPROCESS_ENABLED=true for the "
            + "MAE consumer path. See https://github.com/datahub-project/datahub/issues/19119");
  }
}
