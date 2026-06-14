package com.linkedin.metadata.policy;

import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.annotation.SystemAnnotation;
import com.linkedin.metadata.models.annotation.SystemEntityAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.experimental.UtilityClass;

/** Resolves effective system-entity data visibility from entity and aspect registry specs. */
@UtilityClass
public class SystemDataPolicy {

  /** Returns a cached index of policy decisions for the given registry. */
  @Nonnull
  public static SystemDataPolicyIndex index(@Nonnull final EntityRegistry entityRegistry) {
    return SystemDataPolicyIndex.forRegistry(entityRegistry);
  }

  /**
   * Resolves entity type names for read/search APIs. When {@code requestedEntityNames} is null (all
   * entities), non-read-eligible system entities are omitted when access control is enabled so bulk
   * scroll/search does not fail authorization for the full registry. Explicit entity type requests
   * are returned unchanged.
   */
  @Nonnull
  public static Collection<String> resolveEntityNamesForRead(
      @Nonnull final EntityRegistry entityRegistry,
      @Nullable final Set<String> requestedEntityNames,
      final boolean systemDataAccessControlEnabled) {
    if (requestedEntityNames != null) {
      return requestedEntityNames.stream()
          .map(entityRegistry::getEntitySpec)
          .map(EntitySpec::getName)
          .collect(Collectors.toList());
    }
    SystemDataPolicyIndex policy = index(entityRegistry);
    return entityRegistry.getEntitySpecs().values().stream()
        .map(EntitySpec::getName)
        .filter(name -> !systemDataAccessControlEnabled || policy.isEntityReadEligible(name))
        .collect(Collectors.toList());
  }

  public static boolean effectiveAllowRead(
      @Nonnull final EntitySpec entitySpec, @Nonnull final AspectSpec aspectSpec) {
    SystemAnnotation systemAnnotation = aspectSpec.getSystemAnnotation();
    if (systemAnnotation.isPresent()) {
      return systemAnnotation.isAllowRead();
    }
    return entitySpec.isSystemEntityAllowRead();
  }

  public static boolean effectiveAllowExists(
      @Nonnull final EntitySpec entitySpec, @Nonnull final AspectSpec aspectSpec) {
    if (effectiveAllowRead(entitySpec, aspectSpec)) {
      return true;
    }
    SystemAnnotation systemAnnotation = aspectSpec.getSystemAnnotation();
    if (systemAnnotation.isPresent()) {
      return systemAnnotation.isAllowExists();
    }
    return entitySpec.isSystemEntityAllowExists();
  }

  public static void validateSystemAspectAnnotations(@Nonnull final EntitySpec entitySpec) {
    for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
      if (aspectSpec.getSystemEntityAnnotation().isPresent()
          && !aspectSpec.getName().equals(entitySpec.getKeyAspectName())) {
        throw new com.linkedin.metadata.models.ModelValidationException(
            String.format(
                "@%s is only valid on the key aspect of an entity. Found on aspect %s of entity %s",
                SystemEntityAnnotation.ANNOTATION_NAME,
                aspectSpec.getName(),
                entitySpec.getName()));
      }
      if (aspectSpec.getSystemAnnotation().isPresent() && !entitySpec.isSystemEntity()) {
        throw new com.linkedin.metadata.models.ModelValidationException(
            String.format(
                "@%s is only valid on aspects of system entities. Found on aspect %s of entity %s",
                SystemAnnotation.ANNOTATION_NAME, aspectSpec.getName(), entitySpec.getName()));
      }
    }
  }
}
