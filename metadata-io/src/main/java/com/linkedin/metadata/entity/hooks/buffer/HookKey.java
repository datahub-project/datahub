package com.linkedin.metadata.entity.hooks.buffer;

import java.io.Serializable;

/**
 * Key for one pending post-commit hook replay. Implementations carry the (hookId, urn, aspectName,
 * sequence) identity plus any routing metadata an extension needs to reconstruct the correct
 * {@link io.datahubproject.metadata.context.OperationContext} at drain time (e.g. a cloud
 * multi-tenant impl stamps {@code tenantId} so the drainer routes the replay to the tenant's
 * catalog).
 *
 * <p>Mirrors {@code com.linkedin.metadata.entity.retention.RetentionKey}: an interface with an OSS
 * {@link SimpleHookKey} default and an extension-supplied subtype for routing. Equality MUST
 * include the routing metadata so two replays for the same (hookId, urn, aspect) from different
 * tenants do NOT collapse into one buffer entry — they target different catalogs and must be
 * replayed independently. (The async-only buffer never coalesces anyway — every key is unique via
 * the sequence — but the routing metadata still drives per-tenant grouping at drain.)
 *
 * <p>Implementations must be {@link Serializable} with an explicit {@code serialVersionUID} so
 * in-flight Hazelcast {@code IMap} entries survive rolling deploys (same contract as {@code
 * RetentionKey}).
 */
public interface HookKey extends Serializable {

  /** Plugin-config name of the hook that should replay this MCL. */
  String getHookId();

  /** URN of the entity whose committed MCL is pending. */
  String getUrn();

  /** Aspect name of the committed MCL. */
  String getAspectName();

  /** Globally-unique monotonic enqueue sequence; makes every key distinct (no coalescing). */
  long getSequence();
}
