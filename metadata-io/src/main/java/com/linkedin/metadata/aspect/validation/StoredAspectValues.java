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
 * The string values held by each item's currently stored aspect, by field path, loaded on first use
 * per entity and aspect.
 *
 * <p>A newer version can store values this version's validation rejects (read after a rollback). A
 * validator uses this to keep such a value when an aspect is rewritten with it unchanged at the
 * same field, while still rejecting newly added ones. Paths use the {@code UrnValidationUtil}
 * format: {@code /a/b}, with {@code /*} for the records of a list.
 */
final class StoredAspectValues {

  private final OperationFingerprint operationContext;
  private final RetrieverContext retrieverContext;
  private final BiPredicate<BatchItem, SystemAspect> applies;
  // "urn|aspect" -> "path=value"
  private final Map<String, Set<String>> values = new HashMap<>();

  private StoredAspectValues(
      @Nonnull final OperationFingerprint operationContext,
      @Nonnull final RetrieverContext retrieverContext,
      @Nonnull final BiPredicate<BatchItem, SystemAspect> applies) {
    this.operationContext = operationContext;
    this.retrieverContext = retrieverContext;
    this.applies = applies;
  }

  /**
   * Any stored value counts. For values whose validity is defined in code rather than in the schema
   * (e.g. policy field types), where a newer version adding one doesn't bump the aspect's schema
   * version.
   */
  static StoredAspectValues any(
      @Nonnull final OperationFingerprint operationContext,
      @Nonnull final RetrieverContext retrieverContext) {
    return new StoredAspectValues(operationContext, retrieverContext, (item, stored) -> true);
  }

  /**
   * Only values of an aspect written under a newer schema version count, so invalid data this
   * version or an older one wrote is not kept. For rules the schema defines, such as {@code
   * UrnValidation} entity types.
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

  /** True if the stored aspect holds {@code value} at {@code fieldPath}. */
  boolean contains(
      @Nonnull final BatchItem item, @Nonnull final String fieldPath, @Nonnull final String value) {
    if (item.getUrn() == null || item.getAspectName() == null) {
      return false;
    }
    return values
        .computeIfAbsent(item.getUrn() + "|" + item.getAspectName(), key -> load(item))
        .contains(fieldPath + "=" + value);
  }

  @Nonnull
  private Set<String> load(@Nonnull final BatchItem item) {
    final Set<String> found = new HashSet<>();
    final SystemAspect stored =
        retrieverContext
            .getAspectRetriever()
            .getLatestSystemAspect(operationContext, item.getUrn(), item.getAspectName());
    if (stored != null && stored.getRecordTemplate() != null && applies.test(item, stored)) {
      collect(stored.getRecordTemplate().data(), "", found);
    }
    return found;
  }

  private static void collect(
      @Nullable final Object value, @Nonnull final String path, @Nonnull final Set<String> found) {
    if (value instanceof String string) {
      found.add(path + "=" + string);
    } else if (value instanceof DataMap map) {
      map.forEach((key, child) -> collect(child, path + "/" + key, found));
    } else if (value instanceof DataList list) {
      for (Object item : list) {
        collect(item, item instanceof DataMap ? path + "/*" : path, found);
      }
    }
  }
}
