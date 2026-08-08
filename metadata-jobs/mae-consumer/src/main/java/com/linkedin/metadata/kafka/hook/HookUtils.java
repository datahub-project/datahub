package com.linkedin.metadata.kafka.hook;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.HookExecutionContext;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HookUtils {

  // TODO: Don't need this, just use from EntityKeyUtils
  /**
   * Extracts and returns an {@link Urn} from a {@link MetadataChangeLog}. Extracts from either an
   * entityUrn or entityKey field, depending on which is present.
   */
  public static Urn getUrnFromEvent(
      @Nonnull final MetadataChangeLog event, @Nonnull final EntityRegistry entityRegistry) {
    EntitySpec entitySpec;
    try {
      entitySpec = entityRegistry.getEntitySpec(event.getEntityType());
    } catch (IllegalArgumentException e) {
      log.error("Error while processing entity type {}: {}", event.getEntityType(), e.toString());
      throw new RuntimeException(
          "Failed to get urn from MetadataChangeLog event. Skipping processing.", e);
    }
    // Extract an URN from the Log Event.
    return EntityKeyUtils.getUrnFromLog(event, entitySpec.getKeyAspectSpec());
  }

  /**
   * Records the realized fan-out width of the currently executing hook (RFC-0). The hook name is
   * read ambiently from {@link HookExecutionContext} — matching how external-read counts are
   * attributed — so call sites don't re-derive it. No-op when no {@link MetricUtils} is available.
   */
  public static void recordFanoutSize(@Nonnull final OperationContext opContext, final int size) {
    final String hookName = HookExecutionContext.current().orElse("unknown");
    opContext
        .getMetricUtils()
        .ifPresent(metricUtils -> metricUtils.recordHookFanout(size, hookName));
  }

  private HookUtils() {}
}
