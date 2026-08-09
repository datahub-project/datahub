package com.linkedin.metadata.buffer.offload;

import io.datahubproject.metadata.context.OperationContext;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Feature-specific work the background {@link OffloadDrainer} performs for one drained group of
 * entries, under the group's reconstructed {@link OperationContext}. This is the ONLY use-specific
 * piece a new offload supplies — everything else (drain lock, paging, CAS clear, scheduling, map
 * provisioning) comes from the framework.
 *
 * <p>Implementations must be safe to call on a scheduler thread with no request context: all
 * tenant/routing state must come from the resolved {@code opContext} and the drained entries' keys,
 * never from a thread-local or request scope.
 *
 * <p><b>Entry-removal contract.</b> The action owns the lifecycle of each entry it receives:
 *
 * <ul>
 *   <li>For each entry it finishes successfully, call {@link OffloadBuffer#removeIfSame} so the
 *       entry does not replay next tick.
 *   <li>For an entry to retry, call {@link OffloadBuffer#removeIfSame} then {@link
 *       OffloadBuffer#requeue} (with an updated payload, e.g. a retry count).
 *   <li>For a permanent poison entry, call {@link OffloadBuffer#removeIfSame} and record a metric.
 *   <li>Throwing a transient exception leaves any un-removed entries for the next tick
 *       (at-least-once) — so remove finished entries <em>before</em> throwing. Throwing {@link
 *       UnresolvableOffloadKeyException} signals a permanent whole-group failure and the drainer
 *       drops the group's entries.
 * </ul>
 *
 * @param <K> buffer key type
 * @param <V> buffer payload type
 */
@FunctionalInterface
public interface DrainAction<K extends Serializable, V extends Serializable> {

  /**
   * Replay/apply one drained group, removing finished entries from {@code buffer} (see the
   * entry-removal contract above).
   *
   * @param group the drained entries sharing one {@link OffloadContextResolver#groupKey} routing
   *     context (e.g. one tenant)
   * @param opContext the per-group {@link OperationContext} reconstructed by {@link
   *     OffloadContextResolver#resolveOpContext} (carries the correct tenant/routing)
   * @param buffer the owning buffer, for {@link OffloadBuffer#removeIfSame} / {@link
   *     OffloadBuffer#requeue} of processed entries
   */
  void apply(
      @Nonnull List<Map.Entry<K, V>> group,
      @Nonnull OperationContext opContext,
      @Nonnull OffloadBuffer<K, V> buffer)
      throws Exception;
}
