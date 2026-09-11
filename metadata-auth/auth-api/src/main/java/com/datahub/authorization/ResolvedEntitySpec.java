package com.datahub.authorization;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
  private static final int FIELD_RESOLUTION_TIMEOUT_SECONDS = 5;

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
   * @return a map of propertyUrn -> Set of values, or empty map if none exist.
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
              .get(FIELD_RESOLUTION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
              .getStructuredPropertyValues();
      return structuredPropertyValues != null ? structuredPropertyValues : Collections.emptyMap();
    } catch (TimeoutException e) {
      log.warn("Timeout while resolving structured properties for entity spec: {}", spec, e);
      return Collections.emptyMap();
    } catch (Exception e) {
      log.error("Error while resolving structured properties for entity spec: {}", spec, e);
      return Collections.emptyMap();
    }
  }
}
