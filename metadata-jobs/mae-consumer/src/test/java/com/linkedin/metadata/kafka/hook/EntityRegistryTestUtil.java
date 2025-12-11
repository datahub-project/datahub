/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.kafka.hook;

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
