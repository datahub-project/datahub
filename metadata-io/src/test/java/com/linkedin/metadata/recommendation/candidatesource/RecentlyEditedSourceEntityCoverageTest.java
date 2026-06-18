package com.linkedin.metadata.recommendation.candidatesource;

import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.Constants;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.snapshot.Snapshot;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

/**
 * Registry-derived guard (Category B): the three home-page recommendation candidate sources ({@link
 * RecentlyEditedSource}, {@link RecentlyViewedSource}, {@link MostPopularSource}) all share an
 * identical {@code SUPPORTED_ENTITY_TYPES} set that must equal the entities that declare both the
 * {@code browsePathsV2} and {@code dataPlatformInstance} aspects in the entity registry, minus a
 * documented exception set.
 *
 * <p>Documented exceptions (entities that have both aspects but are intentionally excluded):
 *
 * <ul>
 *   <li>{@code document} — knowledge-article entity; not a data-platform asset and should not
 *       appear on the "recently edited data" home widget.
 *   <li>{@code notebook} — pre-existing omission; notebook has both aspects but was never included.
 *       Tagged as pre-existing to distinguish from a conscious new decision.
 * </ul>
 *
 * <p>We also assert that all three sources have identical lists, so a future update to one does not
 * silently diverge from the others.
 */
public class RecentlyEditedSourceEntityCoverageTest {

  private static final Set<String> INTENTIONAL_EXCLUSIONS =
      Set.of(
          "document", // knowledge-article entity; not a data-platform asset
          "notebook" // pre-existing: has both aspects but historically excluded
          );

  @Test
  public void testSupportedEntityTypesMatchRegistryDerivedSet() {
    final EntityRegistry registry =
        new ConfigEntityRegistry(
            Snapshot.class.getClassLoader().getResourceAsStream("entity-registry.yml"));

    final Set<String> entitiesWithBothAspects =
        registry.getEntitySpecs().values().stream()
            .filter(
                spec ->
                    spec.hasAspect(Constants.BROWSE_PATHS_V2_ASPECT_NAME)
                        && spec.hasAspect(Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME))
            .map(spec -> spec.getName())
            .filter(name -> !INTENTIONAL_EXCLUSIONS.contains(name))
            .collect(Collectors.toCollection(TreeSet::new));

    final Set<String> editedSupported = new TreeSet<>(RecentlyEditedSource.SUPPORTED_ENTITY_TYPES);

    assertEquals(
        editedSupported,
        entitiesWithBothAspects,
        "RecentlyEditedSource.SUPPORTED_ENTITY_TYPES must match entities with both `browsePathsV2` "
            + "and `dataPlatformInstance` aspects in the registry, minus intentional exceptions: "
            + INTENTIONAL_EXCLUSIONS
            + ". registry-derived="
            + entitiesWithBothAspects
            + " configured="
            + editedSupported);
  }

  @Test
  public void testAllThreeSourcesHaveIdenticalSupportedEntityTypes() {
    assertEquals(
        new TreeSet<>(RecentlyEditedSource.SUPPORTED_ENTITY_TYPES),
        new TreeSet<>(RecentlyViewedSource.SUPPORTED_ENTITY_TYPES),
        "RecentlyEditedSource and RecentlyViewedSource must have identical SUPPORTED_ENTITY_TYPES");

    assertEquals(
        new TreeSet<>(RecentlyEditedSource.SUPPORTED_ENTITY_TYPES),
        new TreeSet<>(MostPopularSource.SUPPORTED_ENTITY_TYPES),
        "RecentlyEditedSource and MostPopularSource must have identical SUPPORTED_ENTITY_TYPES");
  }
}
