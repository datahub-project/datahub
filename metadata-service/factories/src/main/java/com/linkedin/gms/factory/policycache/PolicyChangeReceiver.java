package com.linkedin.gms.factory.policycache;

import static com.linkedin.metadata.Constants.POLICY_ENTITY_NAME;

import com.datahub.authorization.DataHubAuthorizer;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Reads into this GMS's policy cache each policy that any GMS signals in {@link
 * com.datahub.authorization.PolicyChangeMap}. Events from this member are not filtered out: the
 * extra key read on the writer is cheap, and it covers writes that this member committed without
 * reading them first.
 */
@Slf4j
public class PolicyChangeReceiver implements AutoCloseable {

  private final DataHubAuthorizer authorizer;
  private final IMap<String, Long> changes;
  @Nullable private final MetricUtils metricUtils;
  @Nullable private volatile UUID listenerId;

  public PolicyChangeReceiver(
      @Nonnull DataHubAuthorizer authorizer,
      @Nonnull IMap<String, Long> changes,
      @Nullable MetricUtils metricUtils) {
    this.authorizer = authorizer;
    this.changes = changes;
    this.metricUtils = metricUtils;
  }

  /**
   * Adds the entry listener first and only then reads the key set, so a change signalled while this
   * member starts is either in the key set or delivered as an event; none falls in between.
   *
   * <p>Never throws, so an unhealthy cluster cannot stop GMS from starting. If the listener cannot
   * be added, this member reads other members' changes on its refresh timer. If only the key-set
   * read fails, the listener stays and a full load replaces the read.
   */
  public void start() {
    try {
      listenerId = changes.addEntryListener(new Listener(), false);
    } catch (RuntimeException e) {
      increment("policyChange.receiver_start_failed", 1);
      log.warn(
          "Could not listen for policy changes from other GMS members; they are read on the"
              + " refresh timer.",
          e);
      return;
    }
    final Set<String> keys;
    try {
      keys = changes.keySet();
    } catch (RuntimeException e) {
      increment("policyChange.receiver_start_failed", 1);
      log.warn("Could not read recent policy changes at startup; requesting a full load.", e);
      authorizer.requestLoad();
      return;
    }
    received(keys);
  }

  @Override
  public void close() {
    final UUID id = listenerId;
    listenerId = null;
    if (id != null) {
      try {
        changes.removeEntryListener(id);
      } catch (RuntimeException e) {
        // The Hazelcast instance may already be shut down, which removes the listener anyway.
        log.debug("Could not remove the policy change listener.", e);
      }
    }
  }

  private void received(@Nonnull Collection<String> keys) {
    if (keys.isEmpty()) {
      return;
    }
    final Set<Urn> urns = new HashSet<>();
    int unreadable = 0;
    for (String key : keys) {
      final Urn urn = policyUrn(key);
      if (urn == null) {
        unreadable++;
        log.warn("Policy change key is not a policy URN: {}", key);
      } else {
        urns.add(urn);
      }
    }
    increment("policyChange.received", keys.size());
    if (unreadable > 0) {
      increment("policyChange.unreadable_key", unreadable);
      // The changed policy is unknown, so only a full load is sure to read it.
      authorizer.requestLoad();
    }
    authorizer.requestUpdate(urns);
  }

  @Nullable
  private static Urn policyUrn(@Nonnull String key) {
    try {
      final Urn urn = Urn.createFromString(key);
      return POLICY_ENTITY_NAME.equals(urn.getEntityType()) ? urn : null;
    } catch (URISyntaxException e) {
      return null;
    }
  }

  private void increment(@Nonnull String name, double count) {
    if (metricUtils != null) {
      metricUtils.increment(PolicyChangeReceiver.class, name, count);
    }
  }

  private final class Listener
      implements EntryAddedListener<String, Long>, EntryUpdatedListener<String, Long> {
    @Override
    public void entryAdded(EntryEvent<String, Long> event) {
      received(Set.of(event.getKey()));
    }

    @Override
    public void entryUpdated(EntryEvent<String, Long> event) {
      received(Set.of(event.getKey()));
    }
  }
}
