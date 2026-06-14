package com.linkedin.test.metadata.models;

import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.ConfigEntitySpec;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import com.linkedin.metadata.models.annotation.SystemAnnotation;
import com.linkedin.metadata.models.annotation.SystemDataVisibility;
import com.linkedin.metadata.models.annotation.SystemEntityAnnotation;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.Value;

/** Shared entity/aspect builders for system data access control unit tests. */
public final class SystemDataEntityFixture {

  private SystemDataEntityFixture() {}

  @Value
  public static class EntityFixture {
    @Nonnull ConfigEntitySpec entitySpec;

    @Nonnull AspectSpec valueAspect;
  }

  @Nonnull
  public static EntityFixture normalEntity(@Nonnull String entityName) {
    AspectSpec key =
        aspect("keyAspect", SystemEntityAnnotation.absent(), SystemAnnotation.absent());
    AspectSpec value =
        aspect("valueAspect", SystemEntityAnnotation.absent(), SystemAnnotation.absent());
    ConfigEntitySpec entity =
        new ConfigEntitySpec(entityName, "keyAspect", List.of(key, value), "default");
    return new EntityFixture(entity, value);
  }

  @Nonnull
  public static EntityFixture systemEntity(
      @Nonnull String entityName, boolean allowRead, boolean allowExists) {
    return systemEntity(entityName, allowRead, allowExists, SystemAnnotation.absent());
  }

  @Nonnull
  public static EntityFixture systemEntity(
      @Nonnull String entityName,
      boolean allowRead,
      boolean allowExists,
      @Nonnull SystemAnnotation valueAspectSystemAnnotation) {
    AspectSpec key =
        aspect(
            "keyAspect",
            new SystemEntityAnnotation(new SystemDataVisibility(true, allowRead, allowExists)),
            SystemAnnotation.absent());
    AspectSpec value =
        aspect("valueAspect", SystemEntityAnnotation.absent(), valueAspectSystemAnnotation);
    ConfigEntitySpec entity =
        new ConfigEntitySpec(entityName, "keyAspect", List.of(key, value), "default");
    entity.setSystemEntityFlags(true, allowRead, allowExists);
    return new EntityFixture(entity, value);
  }

  @Nonnull
  public static AspectSpec aspect(
      @Nonnull String name,
      @Nonnull SystemEntityAnnotation systemEntity,
      @Nonnull SystemAnnotation system) {
    AspectSpec spec =
        new AspectSpec(
            new AspectAnnotation(name, false, false, null, 0),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            null);
    spec.setSystemEntityAnnotation(systemEntity);
    spec.setSystemAnnotation(system);
    return spec;
  }
}
