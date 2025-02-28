package com.datahub.event.hook;

import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;

public class EntityRegistryTestUtil {
  private EntityRegistryTestUtil() {}

  public static final EntityRegistry ENTITY_REGISTRY;

  static {
    EntityRegistryTestUtil.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
    ENTITY_REGISTRY =
        new ConfigEntityRegistry(
            EntityRegistryTestUtil.class
                .getClassLoader()
                .getResourceAsStream("test-entity-registry.yml"));
  }
}
