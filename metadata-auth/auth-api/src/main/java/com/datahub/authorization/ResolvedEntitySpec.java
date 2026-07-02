package com.datahub.authorization;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * Wrapper around authorization request with field resolvers for lazily fetching the field values
 * for each field type
 */
@RequiredArgsConstructor
@ToString
public class ResolvedEntitySpec {
  @Getter private final EntitySpec spec;
  private final Map<EntityFieldType, FieldResolver> fieldResolvers;

  public Set<String> getFieldValues(EntityFieldType entityFieldType) {
    if (!fieldResolvers.containsKey(entityFieldType)) {
      return Collections.emptySet();
    }
    return fieldResolvers.get(entityFieldType).getFieldValuesFuture().join().getValues();
  }

  /**
   * Fetch the typed projection of a field (e.g. {@code Urn} or typed owners) without re-parsing the
   * string {@link #getFieldValues} representation. The element type is determined by the field's
   * resolver; callers must request the type the resolver populates (e.g. {@code Owner} for {@link
   * EntityFieldType#OWNER}).
   *
   * @return the typed values, or an empty set if the field is unresolved or carries no typed data.
   */
  @SuppressWarnings("unchecked")
  public <T> Set<T> getTypedValues(EntityFieldType entityFieldType) {
    if (!fieldResolvers.containsKey(entityFieldType)) {
      return Collections.emptySet();
    }
    return (Set<T>)
        fieldResolvers.get(entityFieldType).getFieldValuesFuture().join().getTypedValues();
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
}
