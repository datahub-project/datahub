package com.linkedin.metadata.graph.cache.service.invalidation;

import static com.linkedin.metadata.Constants.GLOSSARY_NODE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERM_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.service.invalidation.SyncInvalidationPolicy.InvalidationAction;
import org.testng.annotations.Test;

public class SyncInvalidationPolicyTest {

  @Test
  public void forCreateMatchesPolicyTable() {
    assertEquals(
        SyncInvalidationPolicy.forCreate(CacheStatus.ACTIVE, ScopeMode.FULL),
        InvalidationAction.DROP_GRAPH);
    assertEquals(
        SyncInvalidationPolicy.forCreate(CacheStatus.BUILDING, ScopeMode.FULL),
        InvalidationAction.DROP_GRAPH);
    assertEquals(
        SyncInvalidationPolicy.forCreate(CacheStatus.COOLDOWN, ScopeMode.FULL),
        InvalidationAction.DROP_GRAPH);
    assertEquals(
        SyncInvalidationPolicy.forCreate(CacheStatus.ACTIVE, ScopeMode.PARTIAL),
        InvalidationAction.DROP_PARTIAL);
    assertEquals(
        SyncInvalidationPolicy.forCreate(CacheStatus.BUILDING, ScopeMode.PARTIAL),
        InvalidationAction.DROP_PARTIAL);
    assertEquals(
        SyncInvalidationPolicy.forCreate(CacheStatus.ABSENT, ScopeMode.FULL),
        InvalidationAction.DROP_GRAPH);
    assertEquals(
        SyncInvalidationPolicy.forCreate(CacheStatus.ABSENT, ScopeMode.PARTIAL),
        InvalidationAction.DROP_PARTIAL);
    assertEquals(
        SyncInvalidationPolicy.forCreate(CacheStatus.INVALID, ScopeMode.PARTIAL),
        InvalidationAction.NO_OP);
  }

  @Test
  public void forUpdateUsesSurgicalRemoveForStatusAspect() {
    assertEquals(
        SyncInvalidationPolicy.forUpdate(CacheStatus.ACTIVE, ScopeMode.FULL, STATUS_ASPECT_NAME),
        InvalidationAction.SURGICAL_REMOVE);
    assertEquals(
        SyncInvalidationPolicy.forUpdate(
            CacheStatus.INVALID, ScopeMode.PARTIAL, STATUS_ASPECT_NAME),
        InvalidationAction.SURGICAL_REMOVE);
  }

  @Test
  public void forUpdateDelegatesToCreatePolicyForOtherAspects() {
    assertEquals(
        SyncInvalidationPolicy.forUpdate(CacheStatus.ACTIVE, ScopeMode.FULL, "domainProperties"),
        InvalidationAction.DROP_GRAPH);
    assertEquals(
        SyncInvalidationPolicy.forUpdate(CacheStatus.ACTIVE, ScopeMode.PARTIAL, "domainProperties"),
        InvalidationAction.DROP_PARTIAL);
    assertEquals(
        SyncInvalidationPolicy.forUpdate(
            CacheStatus.ACTIVE, ScopeMode.PARTIAL, GLOSSARY_NODE_INFO_ASPECT_NAME),
        InvalidationAction.DROP_PARTIAL);
    assertEquals(
        SyncInvalidationPolicy.forUpdate(
            CacheStatus.ACTIVE, ScopeMode.PARTIAL, GLOSSARY_TERM_INFO_ASPECT_NAME),
        InvalidationAction.DROP_PARTIAL);
    assertEquals(
        SyncInvalidationPolicy.forUpdate(CacheStatus.INVALID, ScopeMode.FULL, "domainProperties"),
        InvalidationAction.NO_OP);
    assertEquals(
        SyncInvalidationPolicy.forUpdate(
            CacheStatus.ABSENT, ScopeMode.FULL, "nativeGroupMembership"),
        InvalidationAction.DROP_GRAPH);
  }

  @Test
  public void forDeleteMatchesPolicyTable() {
    assertEquals(
        SyncInvalidationPolicy.forDelete(CacheStatus.ACTIVE, ScopeMode.FULL),
        InvalidationAction.SURGICAL_REMOVE);
    assertEquals(
        SyncInvalidationPolicy.forDelete(CacheStatus.BUILDING, ScopeMode.FULL),
        InvalidationAction.SURGICAL_REMOVE);
    assertEquals(
        SyncInvalidationPolicy.forDelete(CacheStatus.COOLDOWN, ScopeMode.FULL),
        InvalidationAction.SURGICAL_REMOVE);
    assertEquals(
        SyncInvalidationPolicy.forDelete(CacheStatus.ACTIVE, ScopeMode.PARTIAL),
        InvalidationAction.DROP_PARTIAL);
    assertEquals(
        SyncInvalidationPolicy.forDelete(CacheStatus.BUILDING, ScopeMode.PARTIAL),
        InvalidationAction.DROP_PARTIAL);
    assertEquals(
        SyncInvalidationPolicy.forDelete(CacheStatus.COOLDOWN, ScopeMode.PARTIAL),
        InvalidationAction.DROP_PARTIAL);
    assertEquals(
        SyncInvalidationPolicy.forDelete(CacheStatus.OVER_LIMIT, ScopeMode.PARTIAL),
        InvalidationAction.NO_OP);
    assertEquals(
        SyncInvalidationPolicy.forDelete(CacheStatus.INVALID, ScopeMode.PARTIAL),
        InvalidationAction.NO_OP);
    assertEquals(
        SyncInvalidationPolicy.forDelete(CacheStatus.ABSENT, ScopeMode.FULL),
        InvalidationAction.NO_OP);
  }
}
