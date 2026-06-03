package com.linkedin.metadata.models;

import com.linkedin.metadata.models.annotation.SystemEntityAnnotation;
import com.linkedin.metadata.policy.SystemDataPolicy;
import javax.annotation.Nonnull;

/**
 * Applies {@code @SystemEntity} flags from key-aspect PDL to {@link EntitySpec} implementations.
 */
final class SystemEntitySpecApplier {

  private SystemEntitySpecApplier() {}

  @Nonnull
  static EntitySpec apply(@Nonnull EntitySpec entitySpec) {
    final AspectSpec keyAspect = entitySpec.getKeyAspectSpec();
    final SystemEntityAnnotation keySystemEntity =
        keyAspect != null ? keyAspect.getSystemEntityAnnotation() : SystemEntityAnnotation.absent();
    boolean systemEntity = keySystemEntity.isPresent();
    boolean allowRead = keySystemEntity.isAllowRead();
    boolean allowExists = keySystemEntity.isAllowExists();

    if (entitySpec instanceof DefaultEntitySpec) {
      ((DefaultEntitySpec) entitySpec).setSystemEntityFlags(systemEntity, allowRead, allowExists);
    } else if (entitySpec instanceof ConfigEntitySpec) {
      ((ConfigEntitySpec) entitySpec).setSystemEntityFlags(systemEntity, allowRead, allowExists);
    } else if (entitySpec instanceof PartialEntitySpec) {
      ((PartialEntitySpec) entitySpec).setSystemEntityFlags(systemEntity, allowRead, allowExists);
    }

    SystemDataPolicy.validateSystemAspectAnnotations(entitySpec);
    return entitySpec;
  }
}
