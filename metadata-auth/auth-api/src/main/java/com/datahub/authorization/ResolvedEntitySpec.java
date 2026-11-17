package com.datahub.authorization;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper around authorization request with field resolvers for lazily fetching the field values
 * for each field type
 */
@RequiredArgsConstructor
@ToString
public class ResolvedEntitySpec {
  private static final Logger log = LoggerFactory.getLogger(ResolvedEntitySpec.class);

  @Getter private final EntitySpec spec;
  @Getter private final Map<EntityFieldType, FieldResolver> fieldResolvers;

  public Set<String> getFieldValues(EntityFieldType entityFieldType) {
    if (!fieldResolvers.containsKey(entityFieldType)) {
      return Collections.emptySet();
    }
    return fieldResolvers.get(entityFieldType).getFieldValuesFuture().join().getValues();
  }

  /**
   * Fetch the owners for an entity.
   *
   * @return a set of owner urns, or empty set if none exist.
   */
  public Set<String> getOwners() {
    if (!fieldResolvers.containsKey(EntityFieldType.OWNER)) {
      return Collections.emptySet();
    }
    return fieldResolvers.get(EntityFieldType.OWNER).getFieldValuesFuture().join().getValues();
  }

  /**
   * Fetch the platform instance for a Resolved Resource Spec
   *
   * @return a Platform Instance or null if one does not exist.
   */
  @Nullable
  public String getDataPlatformInstance() {
    if (!fieldResolvers.containsKey(EntityFieldType.DATA_PLATFORM_INSTANCE)) {
      return null;
    }
    Set<String> dataPlatformInstance =
        fieldResolvers
            .get(EntityFieldType.DATA_PLATFORM_INSTANCE)
            .getFieldValuesFuture()
            .join()
            .getValues();
    if (dataPlatformInstance.size() > 0) {
      return dataPlatformInstance.stream().findFirst().get();
    }
    return null;
  }

  /**
   * Fetch the group membership for an entity.
   *
   * @return a set of groups urns, or empty set if none exist.
   */
  public Set<String> getGroupMembership() {
    if (!fieldResolvers.containsKey(EntityFieldType.GROUP_MEMBERSHIP)) {
      return Collections.emptySet();
    }
    return fieldResolvers
        .get(EntityFieldType.GROUP_MEMBERSHIP)
        .getFieldValuesFuture()
        .join()
        .getValues();
  }

  /**
   * Fetch the structured property values for an entity as a map.
   *
   * @return a map of propertyUrn -> Set of values, or empty map if none exist or if resolution
   *     fails. Errors are logged but not propagated to allow authorization to continue with other
   *     policies.
   */
  public Map<String, Set<String>> getStructuredPropertyValues() {
    if (!fieldResolvers.containsKey(EntityFieldType.STRUCTURED_PROPERTY)) {
      return Collections.emptyMap();
    }
    try {
      Map<String, Set<String>> structuredPropertyValues =
          fieldResolvers
              .get(EntityFieldType.STRUCTURED_PROPERTY)
              .getFieldValuesFuture()
              .join()
              .getStructuredPropertyValues();
      return structuredPropertyValues != null ? structuredPropertyValues : Collections.emptyMap();
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      log.warn(
          "Error while resolving structured properties for entity spec {}; skipping structured property evaluation for this criterion",
          spec,
          e);
      // Return empty map on error; this criterion will be skipped
      return Collections.emptyMap();
    }
  }
}
