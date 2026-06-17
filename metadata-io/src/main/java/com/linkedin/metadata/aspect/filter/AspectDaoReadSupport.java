package com.linkedin.metadata.aspect.filter;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.plugins.filter.ReadIntent;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.experimental.UtilityClass;

/**
 * Shared read orchestration for {@link com.linkedin.metadata.entity.AspectDao} implementations.
 * Applies {@link AspectReadGuard} consistently: pre-filter keys or aspect names before storage
 * access when possible, then post-filter loaded rows for defense in depth.
 */
@UtilityClass
public class AspectDaoReadSupport {

  @Nullable
  public static EntityAspect getAspect(
      @Nonnull OperationContext opContext,
      @Nonnull EntityAspectIdentifier key,
      @Nonnull ReadIntent intent,
      @Nonnull Function<EntityAspectIdentifier, EntityAspect> loader) {
    if (!AspectReadGuard.isKeyAllowed(opContext, opContext.getEntityRegistry(), key, intent)) {
      return null;
    }
    EntityAspect raw = loader.apply(key);
    if (raw == null) {
      return null;
    }
    if (!AspectReadGuard.isAspectAllowed(
        opContext,
        opContext.getEntityRegistry(),
        UrnUtils.getUrn(key.getUrn()),
        key.getAspect(),
        raw,
        intent)) {
      return null;
    }
    return raw;
  }

  @Nonnull
  public static Map<EntityAspectIdentifier, EntityAspect> batchGet(
      @Nonnull OperationContext opContext,
      @Nonnull Set<EntityAspectIdentifier> keys,
      boolean forUpdate,
      @Nonnull ReadIntent intent,
      @Nonnull
          BiFunction<
                  Set<EntityAspectIdentifier>, Boolean, Map<EntityAspectIdentifier, EntityAspect>>
              loader) {
    if (keys.isEmpty()) {
      return Collections.emptyMap();
    }
    Set<EntityAspectIdentifier> allowedKeys =
        AspectReadGuard.filterKeys(opContext, opContext.getEntityRegistry(), keys, intent);
    if (allowedKeys.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<EntityAspectIdentifier, EntityAspect> raw = loader.apply(allowedKeys, forUpdate);
    return AspectReadGuard.filterBatchGet(opContext, opContext.getEntityRegistry(), raw, intent);
  }

  /**
   * Range scans load by urn + aspect names + time window. Aspect names are pre-filtered to reduce
   * query scope; returned rows are always post-filtered.
   */
  @Nonnull
  public static List<EntityAspect> getAspectsInRange(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull Set<String> aspectNames,
      @Nonnull ReadIntent intent,
      @Nonnull Function<Set<String>, List<EntityAspect>> loader) {
    if (aspectNames.isEmpty()) {
      return List.of();
    }
    Set<String> allowedAspectNames =
        AspectReadGuard.filterAspectNames(
            opContext, opContext.getEntityRegistry(), urn, aspectNames, intent);
    if (allowedAspectNames.isEmpty()) {
      return List.of();
    }
    List<EntityAspect> raw = loader.apply(allowedAspectNames);
    return new ArrayList<>(
        AspectReadGuard.filterRows(
            opContext,
            opContext.getEntityRegistry(),
            raw,
            intent,
            AspectReadGuard.ENTITY_ASPECT_ROW_ACCESSOR));
  }
}
