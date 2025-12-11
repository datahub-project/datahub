/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.test.metadata.aspect;

import static com.linkedin.metadata.Constants.VERSION_PROPERTIES_ASPECT_NAME;

import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.AspectTemplateEngine;
import com.linkedin.metadata.aspect.patch.template.Template;
import com.linkedin.metadata.aspect.patch.template.common.VersionPropertiesTemplate;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;

public class TestEntityRegistry extends ConfigEntityRegistry {

  static {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  public TestEntityRegistry() {
    super(TestEntityRegistry.class.getClassLoader().getResourceAsStream("entity-registry.yml"));
  }

  public static <T extends RecordTemplate> String getAspectName(T aspect) {
    Map<String, Object> schemaProps = aspect.schema().getProperties();
    if (schemaProps != null && schemaProps.containsKey("Aspect")) {
      Object aspectProps = schemaProps.get("Aspect");
      if (aspectProps instanceof Map aspectMap) {
        return (String) aspectMap.get("name");
      }
    }

    throw new IllegalStateException("Cannot determine aspect name");
  }

  @Nonnull
  @Override
  public AspectTemplateEngine getAspectTemplateEngine() {
    Map<String, Template<? extends RecordTemplate>> aspectTemplateMap = new HashMap<>();
    aspectTemplateMap.put(VERSION_PROPERTIES_ASPECT_NAME, new VersionPropertiesTemplate());
    return new AspectTemplateEngine(aspectTemplateMap);
  }
}
