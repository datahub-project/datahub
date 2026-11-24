package com.linkedin.datahub.graphql.types.timeline;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.datahub.graphql.generated.ChangeCategoryType;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.timeline.TimelineServiceImpl;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Keeps the Change History category filter in the UI in step with the categories the backend
 * registers per entity type, and every registered category resolvable in GraphQL ({@code
 * ChangeEventMapper} uses {@code ChangeCategoryType.valueOf}, so a missing enum value fails the
 * whole getTimeline call).
 */
public class TimelineSupportedCategoriesJsonTest {

  private static final File JSON_FILE =
      new File(
          "../datahub-web-react/src/app/entityV2/shared/tabs/Dataset/Schema/history/"
              + "timelineSupportedCategories.json");

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

  private Map<String, Map<ChangeCategory, Set<String>>> registeredCategories;
  private ConfigEntityRegistry entityRegistry;

  @BeforeClass
  public void setup() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
    registeredCategories =
        new TimelineServiceImpl(mock(AspectDao.class), mock(EntityRegistry.class))
            .getCategoryAspectsByEntityType();
    entityRegistry =
        new ConfigEntityRegistry(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResourceAsStream("entity-registry.yml"),
                "Missing entity-registry.yml"));
  }

  private Map<String, List<String>> expectedCategories() {
    Map<String, List<String>> expected = new TreeMap<>();
    registeredCategories.forEach(
        (entityName, categories) -> {
          EntityType entityType = EntityTypeMapper.getType(entityName);
          if (entityType == EntityType.OTHER) {
            return;
          }
          Set<String> entityAspects =
              entityRegistry.getEntitySpec(entityName).getAspectSpecs().stream()
                  .map(AspectSpec::getName)
                  .collect(Collectors.toSet());
          List<String> supported =
              categories.entrySet().stream()
                  .filter(e -> !Collections.disjoint(e.getValue(), entityAspects))
                  .map(e -> ChangeCategoryType.valueOf(e.getKey().name()).name())
                  .sorted()
                  .collect(Collectors.toList());
          if (!supported.isEmpty()) {
            expected.put(entityType.name(), supported);
          }
        });
    return expected;
  }

  @Test
  public void testFrontendCategoryJsonMatchesBackendRegistry() throws Exception {
    Map<String, List<String>> expected = expectedCategories();
    String expectedJson = OBJECT_MAPPER.writeValueAsString(expected);
    assertTrue(
        JSON_FILE.exists(),
        "Missing " + JSON_FILE + ". Create it with this content:\n" + expectedJson);
    Map<String, List<String>> actual =
        new TreeMap<String, List<String>>(
            OBJECT_MAPPER.readValue(JSON_FILE, new TypeReference<Map<String, List<String>>>() {}));
    assertEquals(
        actual,
        expected,
        JSON_FILE
            + " is out of date with TimelineServiceImpl. Replace its content with:\n"
            + expectedJson);
  }

  @Test
  public void testEveryRegisteredCategoryExistsInGraphql() {
    Set<String> graphqlCategories = new TreeSet<>();
    for (ChangeCategoryType type : ChangeCategoryType.values()) {
      graphqlCategories.add(type.name());
    }
    List<String> missing = new ArrayList<>();
    registeredCategories.forEach(
        (entityName, categories) ->
            categories.entrySet().stream()
                // The legacy per-entity blocks also list categories with no aspects; those never
                // produce events.
                .filter(e -> !e.getValue().isEmpty())
                .map(Map.Entry::getKey)
                .filter(category -> !graphqlCategories.contains(category.name()))
                .forEach(category -> missing.add(entityName + "." + category)));
    assertTrue(
        missing.isEmpty(),
        "Categories registered in TimelineServiceImpl but missing from ChangeCategoryType in"
            + " timeline.graphql: "
            + missing);
  }
}
