package com.linkedin.metadata.models.registry;

import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.util.Pair;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import junit.framework.TestCase;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class RelationshipRegistryTest extends TestCase {

  @BeforeTest
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class.getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @Test
  public void testRegistry() throws EntityRegistryException, IOException {
    final EntityRegistry entityRegistry = new ConfigEntityRegistry(
        "/Users/pedro/dev/oss/personal/datahub/metadata-models/src/main/resources/entity-registry.yml");
    final Map<Pair<String, String>, List<RelationshipFieldSpec>> map = new HashMap<>();
    for (final String entityName : entityRegistry.getEntitySpecs().keySet()) {
      final EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
      final List<AspectSpec> entityAspectSpecs = entitySpec.getAspectSpecs();
      for (AspectSpec entityAspectSpec : entityAspectSpecs) {
        final List<RelationshipFieldSpec> relationships = entityAspectSpec.getRelationshipFieldSpecs();
        relationships.forEach(relationship -> {
          final Pair<String, String> key = Pair.of(entityName, entityAspectSpec.getName());
          final List<RelationshipFieldSpec> specs = map.getOrDefault(key, new ArrayList<>());
          if (specs.contains(relationship)) {
            throw new RuntimeException("spec already exists for key: " + key);
          } else {
            specs.add(relationship);
            map.put(key, specs);
          }
        });
      }
    }
    System.out.println(map);
  }

}