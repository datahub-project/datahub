package com.linkedin.datahub.upgrade.system.sampledata;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import lombok.Data;

/**
 * Represents a sample data manifest file that maps entity URNs to their aspects.
 *
 * <p>Manifest files are stored in /boot/manifests/{version}_manifest.json and are used to calculate
 * diffs between sample data versions to enable incremental migration.
 *
 * <p>Example manifest structure:
 *
 * <pre>
 * {
 *   "version": "001",
 *   "entities": {
 *     "urn:li:dataset:(...)": ["ownership", "schemaMetadata", "status"],
 *     "urn:li:dashboard:(...)": ["dashboardInfo", "ownership"]
 *   }
 * }
 * </pre>
 */
@Data
public class Manifest {

  /** Version identifier (e.g., "001", "002", or hash like "473242af593e") */
  private String version;

  /** Map of entity URN to set of aspect names present in that version */
  private Map<String, Set<String>> entities;

  /** Default constructor for deserialization */
  public Manifest() {
    this.entities = Collections.emptyMap();
  }

  /**
   * Get all entity URNs in this manifest.
   *
   * @return set of entity URNs
   */
  public Set<String> getEntityUrns() {
    return entities.keySet();
  }

  /**
   * Get aspect names for a specific entity URN.
   *
   * @param urn the entity URN
   * @return set of aspect names, or empty set if URN not found
   */
  public Set<String> getAspects(String urn) {
    return entities.getOrDefault(urn, Collections.emptySet());
  }
}
