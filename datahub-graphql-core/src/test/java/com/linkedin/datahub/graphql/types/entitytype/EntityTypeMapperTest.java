package com.linkedin.datahub.graphql.types.entitytype;

import static org.testng.Assert.*;

import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.snapshot.Snapshot;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class EntityTypeMapperTest {

  @Test
  public void testGetType() throws Exception {
    assertEquals(EntityTypeMapper.getType(Constants.DATASET_ENTITY_NAME), EntityType.DATASET);
  }

  @Test
  public void testGetName() throws Exception {
    assertEquals(EntityTypeMapper.getName(EntityType.DATASET), Constants.DATASET_ENTITY_NAME);
  }

  @Test
  public void testGetTypeForDocument() throws Exception {
    assertEquals(EntityTypeMapper.getType(Constants.DOCUMENT_ENTITY_NAME), EntityType.DOCUMENT);
  }

  @Test
  public void testGetNameForDocument() throws Exception {
    assertEquals(EntityTypeMapper.getName(EntityType.DOCUMENT), Constants.DOCUMENT_ENTITY_NAME);
  }

  /**
   * Every registry entity whose name matches a GraphQL EntityType value (e.g. versionSet ->
   * VERSION_SET) must be mapped both ways; otherwise it resolves to OTHER and getName throws.
   */
  @Test
  public void testRegistryEntitiesWithMatchingGraphQLTypeAreMapped() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
    final EntityRegistry registry =
        new ConfigEntityRegistry(
            Snapshot.class.getClassLoader().getResourceAsStream("entity-registry.yml"));
    final Map<String, EntityType> typesByNormalizedName =
        Arrays.stream(EntityType.values())
            .collect(Collectors.toMap(t -> normalize(t.name()), Function.identity()));

    final List<String> unmapped = new ArrayList<>();
    for (String entityName : registry.getEntitySpecs().keySet()) {
      final EntityType type = typesByNormalizedName.get(normalize(entityName));
      if (type == null) {
        continue;
      }
      if (EntityTypeMapper.getType(entityName) != type
          || !EntityTypeMapper.ENTITY_TYPE_TO_NAME.containsKey(type)
          || !EntityTypeMapper.getName(type).equalsIgnoreCase(entityName)) {
        unmapped.add(entityName + " -> " + type);
      }
    }
    assertTrue(unmapped.isEmpty(), "Missing EntityTypeMapper entries: " + unmapped);
  }

  private static String normalize(String name) {
    return name.replace("_", "").toLowerCase();
  }
}
