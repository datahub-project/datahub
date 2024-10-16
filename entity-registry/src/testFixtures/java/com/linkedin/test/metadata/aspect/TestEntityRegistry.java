package com.linkedin.test.metadata.aspect;

import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import java.util.Map;

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
}
