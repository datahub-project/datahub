package com.linkedin.datahub.upgrade.system.sampledata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * Calculates the difference between two sample data manifests.
 *
 * <p>Manifests are JSON files stored in /boot/manifests/ that map entity URNs to their aspects.
 * This calculator loads manifests from the classpath and determines:
 *
 * <ul>
 *   <li>Which entities were removed (present in previous but not in current)
 *   <li>Which entities had aspects removed (need hard-delete + re-ingest)
 *   <li>The complete set of URNs that require hard-deletion
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>
 * ManifestDiffCalculator calculator = new ManifestDiffCalculator();
 * Manifest previous = calculator.loadManifest("001");
 * Manifest current = calculator.loadManifest("002");
 * ManifestDiff diff = calculator.calculateDiff(previous, current);
 * </pre>
 */
@Slf4j
public class ManifestDiffCalculator {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Load a manifest file from the classpath.
   *
   * @param version the manifest version (e.g., "001", "002", "473242af593e")
   * @return the loaded manifest
   * @throws RuntimeException if manifest file cannot be found or parsed
   */
  public Manifest loadManifest(String version) {
    return loadManifestOptional(version)
        .orElseThrow(
            () ->
                new RuntimeException(
                    String.format(
                        "Manifest file not found in classpath: /boot/manifests/%s_manifest.json",
                        version)));
  }

  /**
   * Try to load a manifest file from the classpath, returning empty if not found.
   *
   * <p>This is useful for gracefully handling missing manifests (e.g., when the 001 manifest is
   * removed after all instances have migrated).
   *
   * @param version the manifest version (e.g., "001", "002", "473242af593e")
   * @return Optional containing the manifest, or empty if not found
   */
  public Optional<Manifest> loadManifestOptional(String version) {
    String path = String.format("/boot/manifests/%s_manifest.json", version);
    log.info("Loading manifest from classpath: {}", path);

    try (InputStream is = getClass().getResourceAsStream(path)) {
      if (is == null) {
        log.warn("Manifest file not found in classpath: {}", path);
        return Optional.empty();
      }

      JsonNode root = OBJECT_MAPPER.readTree(is);

      String manifestVersion = root.path("version").asText();
      Map<String, Set<String>> entities = new HashMap<>();

      JsonNode entitiesNode = root.path("entities");
      if (entitiesNode.isObject()) {
        Iterator<String> fieldNames = entitiesNode.fieldNames();
        while (fieldNames.hasNext()) {
          String urn = fieldNames.next();
          JsonNode aspectsArray = entitiesNode.get(urn);

          Set<String> aspects = new HashSet<>();
          if (aspectsArray.isArray()) {
            aspectsArray.forEach(node -> aspects.add(node.asText()));
          }

          entities.put(urn, aspects);
        }
      }

      Manifest manifest = new Manifest();
      manifest.setVersion(manifestVersion);
      manifest.setEntities(entities);

      log.info(
          "Loaded manifest version {} with {} entities and {} total aspects",
          manifestVersion,
          entities.size(),
          entities.values().stream().mapToInt(Set::size).sum());

      return Optional.of(manifest);

    } catch (IOException e) {
      log.error("Failed to parse manifest from classpath: {}", path, e);
      return Optional.empty();
    }
  }

  /**
   * Calculate the difference between two manifests.
   *
   * <p>Determines which entities and aspects have been added, removed, or modified between the
   * previous and current manifest versions.
   *
   * @param previous the previous manifest (can be empty for fresh installs)
   * @param current the current manifest
   * @return the calculated diff
   */
  public ManifestDiff calculateDiff(Manifest previous, Manifest current) {
    log.info(
        "Calculating diff between manifest versions: {} -> {}",
        previous.getVersion(),
        current.getVersion());

    Set<String> previousUrns = previous.getEntityUrns();
    Set<String> currentUrns = current.getEntityUrns();

    // Find entities that were completely removed
    Set<String> removedUrns = Sets.difference(previousUrns, currentUrns).immutableCopy();

    // Find entities that had aspects removed
    Set<String> urnsWithRemovedAspects = new HashSet<>();

    for (String urn : Sets.intersection(previousUrns, currentUrns)) {
      Set<String> prevAspects = previous.getAspects(urn);
      Set<String> currAspects = current.getAspects(urn);

      // Check if any aspects were removed
      Set<String> removedAspects = Sets.difference(prevAspects, currAspects).immutableCopy();
      if (!removedAspects.isEmpty()) {
        urnsWithRemovedAspects.add(urn);
        log.debug(
            "Entity {} had aspects removed: {} -> {}", urn, prevAspects.size(), currAspects.size());
      }
    }

    // Union of both sets = all URNs that need hard-deletion
    Set<String> hardDeleteUrns = Sets.union(removedUrns, urnsWithRemovedAspects).immutableCopy();

    log.info(
        "Diff calculation complete: {} removed URNs, {} URNs with removed aspects, {} total hard deletes",
        removedUrns.size(),
        urnsWithRemovedAspects.size(),
        hardDeleteUrns.size());

    return ManifestDiff.builder()
        .removedUrns(removedUrns)
        .urnsWithRemovedAspects(urnsWithRemovedAspects)
        .hardDeleteUrns(hardDeleteUrns)
        .build();
  }

  /**
   * Calculate the difference for a fresh install (no previous manifest).
   *
   * <p>Returns an empty diff since there's nothing to delete on a fresh install.
   *
   * @param current the current manifest
   * @return empty diff (no deletions needed)
   */
  public ManifestDiff calculateDiffForFreshInstall(Manifest current) {
    log.info("Fresh install detected (no previous manifest), no deletions needed");

    Manifest emptyManifest = new Manifest();
    emptyManifest.setVersion("NONE");
    emptyManifest.setEntities(Collections.emptyMap());

    return calculateDiff(emptyManifest, current);
  }
}
