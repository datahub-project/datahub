package com.linkedin.metadata.timeline;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.eventgenerator.PropertiesDescriptionChangeEventGenerator;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Fails when a glossary term or term group gains an aspect, or its info aspect gains a field, that
 * Change History neither tracks nor deliberately ignores. Fix by registering a generator in {@link
 * TimelineServiceImpl} (or a tracked field on the info generator), or by adding the aspect/field to
 * the ignore list with a reason.
 */
public class TimelineCoverageGuardTest {

  private static final Map<String, Map<String, String>> IGNORED_ASPECTS =
      Map.of(
          GLOSSARY_TERM_ENTITY_NAME,
          Map.of(
              "schemaMetadata", "unused on glossary terms",
              "displayProperties", "cosmetic",
              "forms", "compliance workflow, not a term change",
              "testResults", "system-computed",
              "browsePaths", "system-computed",
              "assetSettings", "system setting",
              "subTypes", "system-assigned",
              "glossaryTermKey", "key aspect"),
          GLOSSARY_NODE_ENTITY_NAME,
          Map.of(
              "status", "soft-delete is out of Change History scope",
              "displayProperties", "cosmetic",
              "forms", "compliance workflow, not a term change",
              "testResults", "system-computed",
              "assetSettings", "system setting",
              "subTypes", "system-assigned",
              "glossaryNodeKey", "key aspect"));

  private EntityRegistry entityRegistry;
  private TimelineServiceImpl timelineService;

  @BeforeClass
  public void setup() {
    entityRegistry =
        new ConfigEntityRegistry(
            Snapshot.class.getClassLoader().getResourceAsStream("entity-registry.yml"));
    timelineService = new TimelineServiceImpl(mock(AspectDao.class), mock(EntityRegistry.class));
  }

  private Set<String> registryAspects(String entityType) {
    return entityRegistry.getEntitySpec(entityType).getAspectSpecs().stream()
        .map(AspectSpec::getName)
        .collect(Collectors.toSet());
  }

  private Set<String> trackedAspects(String entityType) {
    return timelineService.getCategoryAspectsByEntityType().get(entityType).values().stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toSet());
  }

  @Test
  public void testEveryGlossaryAspectIsTrackedOrIgnored() {
    Set<String> unclassified = new TreeSet<>();
    IGNORED_ASPECTS.forEach(
        (entityType, ignored) -> {
          Set<String> missing = new HashSet<>(registryAspects(entityType));
          missing.removeAll(trackedAspects(entityType));
          missing.removeAll(ignored.keySet());
          missing.forEach(aspect -> unclassified.add(entityType + "." + aspect));
        });
    assertTrue(
        unclassified.isEmpty(),
        "Aspects not classified for Change History: "
            + unclassified
            + ". Register a generator in TimelineServiceImpl or add the aspect to IGNORED_ASPECTS"
            + " with a reason.");
  }

  @Test
  public void testNoStaleIgnoredAspects() {
    Set<String> stale = new TreeSet<>();
    IGNORED_ASPECTS.forEach(
        (entityType, ignored) -> {
          Set<String> aspects = registryAspects(entityType);
          ignored.keySet().stream()
              .filter(aspect -> !aspects.contains(aspect))
              .forEach(aspect -> stale.add(entityType + "." + aspect));
        });
    assertTrue(stale.isEmpty(), "IGNORED_ASPECTS entries not in entity-registry.yml: " + stale);
  }

  @Test
  public void testEveryGlossaryInfoFieldIsTrackedOrIgnored() {
    assertInfoFieldsClassified(
        GLOSSARY_TERM_ENTITY_NAME, GLOSSARY_TERM_INFO_ASPECT_NAME, new GlossaryTermInfo());
    assertInfoFieldsClassified(
        GLOSSARY_NODE_ENTITY_NAME, GLOSSARY_NODE_INFO_ASPECT_NAME, new GlossaryNodeInfo());
  }

  private void assertInfoFieldsClassified(
      String entityType, String aspectName, RecordTemplate emptyAspect) {
    Set<String> classified = new TreeSet<>();
    for (ChangeCategory category : Set.of(ChangeCategory.DOCUMENTATION, ChangeCategory.PARENT)) {
      PropertiesDescriptionChangeEventGenerator<?> generator =
          (PropertiesDescriptionChangeEventGenerator<?>)
              timelineService.getTimelineGenerator(entityType, category, aspectName);
      classified.addAll(generator.getTrackedFieldNames());
      classified.addAll(generator.getIgnoredFields().keySet());
    }
    Set<String> fields =
        ((RecordDataSchema) emptyAspect.schema())
            .getFields().stream()
                .map(RecordDataSchema.Field::getName)
                .collect(Collectors.toCollection(TreeSet::new));
    assertEquals(
        classified,
        fields,
        entityType
            + "."
            + aspectName
            + " fields must each be tracked by its Change History generator or listed in its"
            + " ignoredFields with a reason.");
  }
}
