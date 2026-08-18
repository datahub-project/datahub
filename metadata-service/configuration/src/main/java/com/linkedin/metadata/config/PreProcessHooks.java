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
   * Rejects the silent-drop configuration from issue 19119: this process consumes MCLs and both UI
   * indexing paths are off. UI writes land in primary storage, the MAE hook skips {@code
   * appSource=ui}, and search never sees the entity.
   *
   * <p>Call only from GMS (embedded MCL consumer). Standalone MAE is not validated here — helm sets
   * {@code PRE_PROCESS_HOOKS_UI_ENABLED=false} on MAE when GMS preprocess is on.
   *
   * <p>Kubernetes scale-down sets preprocess off and {@code MAE_CONSUMER_ENABLED=false}, so this
   * check does not fire during system-update.
   *
   * @param hooks preprocess flag pair; {@code null} is treated as both paths disabled
   * @param mclConsumerEnabled true when {@code MAE_CONSUMER_ENABLED} or {@code
   *     MCL_CONSUMER_ENABLED} is true
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
