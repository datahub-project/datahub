package com.linkedin.metadata.aspect.validation;

import com.datahub.context.OperationFingerprint;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.models.registry.RegistryKnowledge;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The string values held by each item's currently stored aspect, loaded on first use per item.
 *
 * <p>A newer version can store values this version's validation rejects (read after a rollback). A
 * validator uses this to keep such a value when an aspect is rewritten with it unchanged, while
 * still rejecting newly added ones.
 */
final class StoredAspectValues {

  private final OperationFingerprint operationContext;
  private final RetrieverContext retrieverContext;
  private final BiPredicate<BatchItem, SystemAspect> applies;
  private final Map<BatchItem, Set<String>> values = new HashMap<>();

  private StoredAspectValues(
      @Nonnull final OperationFingerprint operationContext,
      @Nonnull final RetrieverContext retrieverContext,
      @Nonnull final BiPredicate<BatchItem, SystemAspect> applies) {
    this.operationContext = operationContext;
    this.retrieverContext = retrieverContext;
    this.applies = applies;
  }

  /** Any stored value counts. */
  static StoredAspectValues any(
      @Nonnull final OperationFingerprint operationContext,
      @Nonnull final RetrieverContext retrieverContext) {
    return new StoredAspectValues(operationContext, retrieverContext, (item, stored) -> true);
  }

  /**
   * Only values of an aspect written under a newer schema version count, so invalid data this
   * version or an older one wrote is not kept.
   */
  static StoredAspectValues writtenByNewerSchema(
      @Nonnull final OperationFingerprint operationContext,
      @Nonnull final RetrieverContext retrieverContext) {
    return new StoredAspectValues(
        operationContext,
        retrieverContext,
        (item, stored) ->
            RegistryKnowledge.isWrittenByNewerSchema(
                stored.getSystemMetadata(), item.getAspectSpec()));
  }

  boolean contains(@Nonnull final BatchItem item, @Nonnull final String value) {
    return values.computeIfAbsent(item, this::load).contains(value);
  }

  @Nonnull
  private Set<String> load(@Nonnull final BatchItem item) {
    final Set<String> found = new HashSet<>();
    if (item.getUrn() == null || item.getAspectName() == null) {
      return found;
    }
    final SystemAspect stored =
        retrieverContext
            .getAspectRetriever()
            .getLatestSystemAspect(operationContext, item.getUrn(), item.getAspectName());
    if (stored != null && stored.getRecordTemplate() != null && applies.test(item, stored)) {
      collectStrings(stored.getRecordTemplate().data(), found);
    }
    return found;
  }

  private static void collectStrings(
      @Nullable final Object value, @Nonnull final Set<String> found) {
    if (value instanceof String string) {
      found.add(string);
    } else if (value instanceof DataMap map) {
      map.values().forEach(child -> collectStrings(child, found));
    } else if (value instanceof DataList list) {
      list.forEach(child -> collectStrings(child, found));
    }
  }
}
