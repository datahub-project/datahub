package com.datahub.authorization;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;


/**
 * Wrapper around authorization request with field resolvers for lazily fetching the field values for each field type
 */
@RequiredArgsConstructor
public class ResolvedResourceSpec {
  @Getter
  private final ResourceSpec spec;
  private final Map<ResourceFieldType, FieldResolver> fieldResolvers;

  public Set<String> getFieldValues(ResourceFieldType resourceFieldType) {
    if (!fieldResolvers.containsKey(resourceFieldType)) {
      return Collections.emptySet();
    }
    return fieldResolvers.get(resourceFieldType).getFieldValuesFuture().join().getValues();
  }

  /**
   * Fetch the entity-registry type for a resource. ('dataset', 'dashboard', 'chart').
   * @return the entity type.
   */
  public String getType() {
    if (!fieldResolvers.containsKey(ResourceFieldType.RESOURCE_TYPE)) {
      throw new UnsupportedOperationException(
          "Failed to resolve resource type! No field resolver for RESOURCE_TYPE provided.");
    }
    Set<String> resourceTypes =
        fieldResolvers.get(ResourceFieldType.RESOURCE_TYPE).getFieldValuesFuture().join().getValues();
    assert resourceTypes.size() == 1; // There should always be a single resource type.
    return resourceTypes.stream().findFirst().get();
  }

  /**
   * Fetch the owners for a resource.
   * @return a set of owner urns, or empty set if none exist.
   */
  public Set<String> getOwners() {
    if (!fieldResolvers.containsKey(ResourceFieldType.OWNER)) {
      return Collections.emptySet();
    }
    return fieldResolvers.get(ResourceFieldType.OWNER).getFieldValuesFuture().join().getValues();
  }

  /**
   * Fetch the domain for a Resolved Resource Spec
   * @return a Domain or null if one does not exist.
   */
  @Nullable
  public String getDomain() {
    if (!fieldResolvers.containsKey(ResourceFieldType.DOMAIN)) {
      return null;
    }
    Set<String> domains = fieldResolvers.get(ResourceFieldType.DOMAIN).getFieldValuesFuture().join().getValues();
    if (domains.size() > 0) {
      return domains.stream().findFirst().get();
    }
    return null;
  }
}
