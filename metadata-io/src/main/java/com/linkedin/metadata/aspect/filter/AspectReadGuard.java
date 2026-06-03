package com.linkedin.metadata.aspect.filter;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.plugins.filter.AspectReadContext;
import com.linkedin.metadata.aspect.plugins.filter.AspectReadFilter;
import com.linkedin.metadata.aspect.plugins.filter.ReadIntent;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.experimental.UtilityClass;

@UtilityClass
public class AspectReadGuard {

  public static final RowAccessor<EntityAspect> ENTITY_ASPECT_ROW_ACCESSOR =
      new RowAccessor<EntityAspect>() {
        @Override
        public @Nonnull String getUrn(EntityAspect row) {
          return row.getUrn();
        }

        @Override
        public @Nonnull String getAspectName(EntityAspect row) {
          return row.getAspect();
        }

        @Override
        public EntityAspect toEntityAspect(EntityAspect row) {
          return row;
        }
      };

  /**
   * Pre-filter for key-addressable reads ({@code getAspect}, {@code batchGet}). Skips datastore
   * work when the key is denied without needing the aspect payload.
   */
  public static boolean isKeyAllowed(
      @Nonnull OperationContext opContext,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull EntityAspectIdentifier key,
      @Nonnull ReadIntent intent) {
    return isAllowed(opContext, entityRegistry, key, null, intent);
  }

  @Nonnull
  public static Set<EntityAspectIdentifier> filterKeys(
      @Nonnull OperationContext opContext,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull Set<EntityAspectIdentifier> keys,
      @Nonnull ReadIntent intent) {
    if (shouldSkipFiltering(opContext)) {
      return keys;
    }
    return keys.stream()
        .filter(key -> isKeyAllowed(opContext, entityRegistry, key, intent))
        .collect(Collectors.toSet());
  }

  /**
   * Pre-filter aspect names for range scans ({@code getAspectsInRange}). Narrows the query when
   * possible; callers still post-filter rows because range queries cannot always express policy.
   */
  @Nonnull
  public static Set<String> filterAspectNames(
      @Nonnull OperationContext opContext,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull Urn urn,
      @Nonnull Set<String> aspectNames,
      @Nonnull ReadIntent intent) {
    if (shouldSkipFiltering(opContext)) {
      return aspectNames;
    }
    return aspectNames.stream()
        .filter(aspectName -> isAllowed(opContext, entityRegistry, urn, aspectName, null, intent))
        .collect(Collectors.toSet());
  }

  @Nonnull
  public static Map<EntityAspectIdentifier, EntityAspect> filterBatchGet(
      @Nonnull OperationContext opContext,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull Map<EntityAspectIdentifier, EntityAspect> raw,
      @Nonnull ReadIntent intent) {
    if (shouldSkipFiltering(opContext)) {
      return raw;
    }
    return raw.entrySet().stream()
        .filter(
            entry -> isAllowed(opContext, entityRegistry, entry.getKey(), entry.getValue(), intent))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Nonnull
  public static <T> Collection<T> filterRows(
      @Nonnull OperationContext opContext,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull Collection<T> rows,
      @Nonnull ReadIntent intent,
      @Nonnull RowAccessor<T> rowAccessor) {
    if (shouldSkipFiltering(opContext)) {
      return rows;
    }
    return rows.stream()
        .filter(
            row ->
                isAllowed(
                    opContext,
                    entityRegistry,
                    UrnUtils.getUrn(rowAccessor.getUrn(row)),
                    rowAccessor.getAspectName(row),
                    rowAccessor.toEntityAspect(row),
                    intent))
        .collect(Collectors.toList());
  }

  public interface RowAccessor<T> {
    @Nonnull
    String getUrn(T row);

    @Nonnull
    String getAspectName(T row);

    @Nullable
    EntityAspect toEntityAspect(T row);
  }

  public static boolean isAspectAllowed(
      @Nonnull OperationContext opContext,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      @Nullable EntityAspect entityAspect,
      @Nonnull ReadIntent intent) {
    return isAllowed(opContext, entityRegistry, urn, aspectName, entityAspect, intent);
  }

  private static boolean shouldSkipFiltering(@Nonnull OperationContext opContext) {
    return opContext.isSystemAuth();
  }

  private static boolean isAllowed(
      @Nonnull OperationContext opContext,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull EntityAspectIdentifier key,
      @Nullable EntityAspect aspect,
      @Nonnull ReadIntent intent) {
    return isAllowed(
        opContext, entityRegistry, UrnUtils.getUrn(key.getUrn()), key.getAspect(), aspect, intent);
  }

  private static boolean isAllowed(
      @Nonnull OperationContext opContext,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      @Nullable EntityAspect entityAspect,
      @Nonnull ReadIntent intent) {
    if (shouldSkipFiltering(opContext)) {
      return true;
    }

    AspectReadContext context =
        AspectReadContext.builder()
            .urn(urn)
            .aspectName(aspectName)
            .intent(intent)
            .entityAspect(entityAspect)
            .build();

    for (AspectReadFilter filter : entityRegistry.getAllAspectReadFilters()) {
      if (filter.shouldApply(intent, urn.getEntityType(), aspectName)
          && !filter.isAllowed(context, entityRegistry)) {
        return false;
      }
    }
    return true;
  }
}
