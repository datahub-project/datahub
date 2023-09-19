package com.datahub.authorization;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;


/**
 * Wrapper around authorization request with field resolvers for lazily fetching the field values for each field type
 */
@RequiredArgsConstructor
@ToString
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
   * Fetch the owners for a resource.
   * @return a set of owner urns, or empty set if none exist.
   */
  public Set<String> getOwners() {
    if (!fieldResolvers.containsKey(ResourceFieldType.OWNER)) {
      return Collections.emptySet();
    }
    return fieldResolvers.get(ResourceFieldType.OWNER).getFieldValuesFuture().join().getValues();
  }
}
