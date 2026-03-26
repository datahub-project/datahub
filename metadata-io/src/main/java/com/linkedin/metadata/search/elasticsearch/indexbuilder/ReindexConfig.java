package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.search.utils.ESUtils.PROPERTIES;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.linkedin.util.Pair;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.common.settings.Settings;

@Slf4j
@Builder
@Getter
@Accessors(fluent = true)
public class ReindexConfig {

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    OBJECT_MAPPER
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
  }

  /*
    Most index settings are default values and populated by Elastic. This list is an include list to determine which
    settings we care about when a difference is present.
  */
  public static final List<String> SETTINGS_DYNAMIC = ImmutableList.of("refresh_interval");
  // These setting require reindex
  public static final List<String> SETTINGS_STATIC = ImmutableList.of("number_of_shards");
  public static final List<String> SETTINGS =
      Stream.concat(SETTINGS_DYNAMIC.stream(), SETTINGS_STATIC.stream())
          .collect(Collectors.toList());

  private final String name;
  private final boolean exists;
  private final Settings currentSettings;
  private final Map<String, Object> targetSettings;
  private final Map<String, Object> currentMappings;
  private final Map<String, Object> targetMappings;
  private final boolean enableIndexMappingsReindex;
  private final boolean enableIndexSettingsReindex;
  private final boolean enableStructuredPropertiesReindex;
  private final String version;

  /* Calculated */
  private boolean requiresReindex;
  private boolean requiresApplySettings;
  private boolean requiresApplyMappings;
  private final boolean isPureMappingsAddition;
  private final boolean isSettingsReindex;
  private final boolean hasNewStructuredProperty;
  private final boolean isPureStructuredPropertyAddition;
  private final boolean hasRemovedStructuredProperty;

  private void restrictedMethod() throws IllegalAccessException {
    String allowed = "ReindexDebugStep";
    if (!isCalledFromReindexDebugStep(allowed)) {
      throw new IllegalAccessException("This method can only be called from " + allowed);
    }
  }

  private boolean isCalledFromReindexDebugStep(String step) {
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    return Arrays.stream(stackTrace)
        .anyMatch(
            element -> {
              try {
                return Class.forName(element.getClassName()).getSimpleName().equals(step);
              } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
              }
            });
  }

  public void forceReindex() throws IllegalAccessException {
    restrictedMethod();
    requiresReindex = true;
    requiresApplyMappings = true;
    requiresApplySettings = true;
  }

  public static ReindexConfigBuilder builder() {
    return new CalculatedBuilder();
  }

  public static class ReindexConfigBuilder {

    // hide calculated fields
    private ReindexConfigBuilder requiresReindex(boolean ignored) {
      return this;
    }

    private ReindexConfigBuilder requiresApplySettings(boolean ignored) {
      return this;
    }

    private ReindexConfigBuilder requiresApplyMappings(boolean ignored) {
      return this;
    }

    private ReindexConfigBuilder isPureMappingsAddition(boolean ignored) {
      return this;
    }

    private ReindexConfigBuilder isSettingsReindexRequired(boolean ignored) {
      return this;
    }

    private ReindexConfigBuilder hasNewStructuredProperty(boolean ignored) {
      return this;
    }

    private ReindexConfigBuilder isPureStructuredPropertyAddition(boolean ignored) {
      return this;
    }

    private ReindexConfigBuilder hasRemovedStructuredProperty(boolean ignored) {
      return this;
    }

    // ensure sorted
    public ReindexConfigBuilder currentMappings(Map<String, Object> currentMappings) {
      this.currentMappings = sortMap(currentMappings);
      return this;
    }

    public ReindexConfigBuilder targetMappings(Map<String, Object> targetMappings) {
      this.targetMappings = sortMap(targetMappings);
      return this;
    }

    private static TreeMap<String, Object> sortMap(Map<String, Object> input) {
      if (input == null) {
        return new TreeMap<>();
      }
      return input.entrySet().stream()
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey,
                  e -> sortObject(e.getValue()),
                  (oldValue, newValue) -> newValue,
                  TreeMap::new));
    }

    private static List<Object> sortList(List<?> input) {
      if (input == null) {
        return new ArrayList<>();
      }
      return input.stream().map(ReindexConfigBuilder::sortObject).collect(Collectors.toList());
    }

    private static Object sortObject(Object item) {
      if (item instanceof Map) {
        return sortMap((Map<String, Object>) item);
      } else if (item instanceof List) {
        return sortList((List<?>) item);
      } else {
        return item;
      }
    }

    /**
     * Normalize a map for comparison by recursively converting all primitive values to strings.
     * This ensures consistent comparison between mappings from different sources (code vs
     * Elasticsearch).
     */
    static TreeMap<String, Object> normalizeMapForComparison(Map<String, Object> input) {
      if (input == null) {
        return new TreeMap<>();
      }
      return input.entrySet().stream()
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey,
                  e -> normalizeObjectForComparison(e.getValue()),
                  (oldValue, newValue) -> newValue,
                  TreeMap::new));
    }

    private static List<Object> normalizeListForComparison(List<?> input) {
      if (input == null) {
        return new ArrayList<>();
      }
      return input.stream()
          .map(ReindexConfigBuilder::normalizeObjectForComparison)
          .collect(Collectors.toList());
    }

    private static Object normalizeObjectForComparison(Object item) {
      if (item instanceof Map) {
        return normalizeMapForComparison((Map<String, Object>) item);
      } else if (item instanceof List) {
        return normalizeListForComparison((List<?>) item);
      } else {
        return String.valueOf(item);
      }
    }
  }

  /** Implement calculated fields */
  public String indexPattern() {
    return name + "*";
  }

  public String indexCleanPattern() {
    return name + "_*";
  }

  private static class CalculatedBuilder extends ReindexConfigBuilder {

    @Override
    public ReindexConfig build() {
      if (super.exists) {
        /* Consider mapping changes */
        MapDifference<String, Object> mappingsDiff =
            calculateMapDifference(
                normalizeMapForComparison(getOrDefault(super.currentMappings, List.of(PROPERTIES))),
                normalizeMapForComparison(getOrDefault(super.targetMappings, List.of(PROPERTIES))));

        super.requiresApplyMappings =
            !mappingsDiff.entriesDiffering().isEmpty()
                || !mappingsDiff.entriesOnlyOnRight().isEmpty();
        super.isPureStructuredPropertyAddition =
            mappingsDiff
                    .entriesDiffering()
                    .keySet()
                    .equals(Set.of(STRUCTURED_PROPERTY_MAPPING_FIELD))
                || mappingsDiff
                    .entriesOnlyOnRight()
                    .keySet()
                    .equals(Set.of(STRUCTURED_PROPERTY_MAPPING_FIELD));
        super.isPureMappingsAddition =
            super.requiresApplyMappings
                && mappingsDiff.entriesDiffering().isEmpty()
                && !mappingsDiff.entriesOnlyOnRight().isEmpty();
        super.hasNewStructuredProperty =
            (mappingsDiff.entriesDiffering().containsKey(STRUCTURED_PROPERTY_MAPPING_FIELD)
                    || mappingsDiff
                        .entriesOnlyOnRight()
                        .containsKey(STRUCTURED_PROPERTY_MAPPING_FIELD))
                && structuredPropertiesDiffCount(super.currentMappings, super.targetMappings)
                        .getSecond()
                    > 0;
        super.hasRemovedStructuredProperty =
            (mappingsDiff.entriesDiffering().containsKey(STRUCTURED_PROPERTY_MAPPING_FIELD)
                    || mappingsDiff
                        .entriesOnlyOnLeft()
                        .containsKey(STRUCTURED_PROPERTY_MAPPING_FIELD))
                && structuredPropertiesDiffCount(super.currentMappings, super.targetMappings)
                        .getFirst()
                    > 0;

        if (super.requiresApplyMappings && super.isPureMappingsAddition) {
          log.info(
              "Index: {} - New fields have been added to index. Adding: {}",
              super.name,
              mappingsDiff.entriesOnlyOnRight());
        } else if (super.requiresApplyMappings) {
          log.info(
              "Index: {} - There's diff between new mappings (left) and old mappings (right): {}",
              super.name,
              mappingsDiff.entriesDiffering());
        }

        /* Consider analysis and settings changes */
        super.requiresApplySettings = !isSettingsEqual() || !isAnalysisEqual();
        super.isSettingsReindex = isSettingsReindexRequired();

        /* Determine reindexing required - some settings and mappings do not require reindex, analysis always does */
        if (super.requiresApplyMappings && !super.isPureMappingsAddition) {
          if (super.enableIndexMappingsReindex) {
            super.requiresReindex = true;
          } else {
            log.warn(
                "Index: {} - There's diff between new mappings, however reindexing is DISABLED.",
                super.name);
          }
        } else if (super.hasRemovedStructuredProperty) {
          if (super.enableIndexMappingsReindex
              && super.enableIndexMappingsReindex
              && super.enableStructuredPropertiesReindex) {
            super.requiresApplyMappings = true;
            super.requiresReindex = true;
          } else {
            if (!super.enableIndexMappingsReindex) {
              log.warn(
                  "Index: {} - There's diff between new mappings, however reindexing is DISABLED.",
                  super.name);
            }
            if (!super.enableIndexMappingsReindex) {
              log.warn(
                  "Index: {} - There's a removed Structured Property, however Structured Property reindexing is DISABLED.",
                  super.name);
            }
          }
        }

        if (super.isSettingsReindex) {
          try {
            if (!isAnalysisEqual()) {
              log.info(
                  "Index: {} - There's an update to `analysis` settings that requires reindexing. Target: {} Current: {}",
                  super.name,
                  OBJECT_MAPPER.writeValueAsString(super.targetSettings),
                  super.currentSettings);
            }
            if (!isSettingsEqual()) {
              log.info(
                  "Index: {} - There's an update to settings that requires reindexing. Target: {} Current: {}",
                  super.name,
                  OBJECT_MAPPER.writeValueAsString(super.targetSettings),
                  super.currentSettings);
            }
          } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
          }
          if (super.enableIndexSettingsReindex) {
            super.requiresReindex = true;
          } else {
            log.warn(
                "Index: {} - There's an update to settings that requires reindexing, however reindexing is DISABLED",
                super.name);
          }
        }
      }
      return super.build();
    }

    private static TreeMap<String, Object> getOrDefault(
        Map<String, Object> map, List<String> path) {
      if (map == null) {
        return new TreeMap<>();
      }

      TreeMap<String, Object> item =
          (TreeMap<String, Object>) map.getOrDefault(path.get(0), new TreeMap());
      if (path.size() == 1) {
        return item;
      } else {
        return getOrDefault(item, path.subList(1, path.size()));
      }
    }

    /**
     * Return counts for removed and added structured properties based on the difference between the
     * existing mapping configuration and the target configuration
     *
     * @return count of structured properties to be removed and added to the index mapping
     */
    private static Pair<Long, Long> structuredPropertiesDiffCount(
        Map<String, Object> current, Map<String, Object> target) {
      Set<String> currentStructuredProperties = new HashSet<>();
      Set<String> targetStructuredProperties = new HashSet<>();

      // add non-versioned property ids
      currentStructuredProperties.addAll(
          getOrDefault(
                  current, List.of("properties", STRUCTURED_PROPERTY_MAPPING_FIELD, "properties"))
              .keySet()
              .stream()
              .filter(k -> !STRUCTURED_PROPERTY_MAPPING_VERSIONED_FIELD.equals(k))
              .collect(Collectors.toSet()));
      targetStructuredProperties.addAll(
          getOrDefault(
                  target, List.of("properties", STRUCTURED_PROPERTY_MAPPING_FIELD, "properties"))
              .keySet()
              .stream()
              .filter(k -> !STRUCTURED_PROPERTY_MAPPING_VERSIONED_FIELD.equals(k))
              .collect(Collectors.toSet()));

      // Extract versioned/typed property ids
      currentStructuredProperties.addAll(getVersionedStructuredPropertyIds(current));
      targetStructuredProperties.addAll(getVersionedStructuredPropertyIds(target));

      return Pair.of(
          currentStructuredProperties.stream()
              .filter(p -> !targetStructuredProperties.contains(p))
              .count(),
          targetStructuredProperties.stream()
              .filter(p -> !currentStructuredProperties.contains(p))
              .count());
    }

    private boolean isAnalysisEqual() {
      if (super.targetSettings == null || !super.targetSettings.containsKey("index")) {
        return true;
      }
      Map<String, Object> indexSettings = (Map<String, Object>) super.targetSettings.get("index");
      if (!indexSettings.containsKey("analysis")) {
        return true;
      }
      // Compare analysis section
      Map<String, Object> newAnalysis = (Map<String, Object>) indexSettings.get("analysis");
      Settings oldAnalysis = super.currentSettings.getByPrefix("index.analysis.");
      return equalsGroup(newAnalysis, oldAnalysis);
    }

    private boolean isSettingsEqual() {
      if (super.targetSettings == null || !super.targetSettings.containsKey("index")) {
        return true;
      }
      Map<String, Object> indexSettings = (Map<String, Object>) super.targetSettings.get("index");
      return SETTINGS.stream()
          .allMatch(
              settingKey -> {
                Object targetValue = indexSettings.get(settingKey);
                String currentValue = super.currentSettings.get("index." + settingKey);
                // Handle null values properly
                if (targetValue == null && currentValue == null) {
                  return true;
                }
                if (targetValue == null || currentValue == null) {
                  return false;
                }
                return Objects.equals(targetValue.toString(), currentValue);
              });
    }

    private boolean isSettingsReindexRequired() {
      if (super.targetSettings == null || !super.targetSettings.containsKey("index")) {
        return false;
      }
      Map<String, Object> indexSettings = (Map<String, Object>) super.targetSettings.get("index");

      if (SETTINGS_STATIC.stream()
          .anyMatch(
              settingKey -> {
                Object targetValue = indexSettings.get(settingKey);
                String currentValue = super.currentSettings.get("index." + settingKey);
                // Handle null values properly
                if (targetValue == null && currentValue == null) {
                  return false;
                }
                if (targetValue == null || currentValue == null) {
                  return true;
                }
                return !Objects.equals(targetValue.toString(), currentValue);
              })) {
        return true;
      }

      return (indexSettings.containsKey("analysis")
          && !equalsGroup(
              (Map<String, Object>) indexSettings.get("analysis"),
              super.currentSettings.getByPrefix("index.analysis.")));
    }

    /**
     * Dynamic fields should not be considered as part of the difference, except for structured
     * properties. This prevents unnecessary reindexing when dynamic fields have minor differences
     * in their properties.
     *
     * @param currentMappings current mappings
     * @param targetMappings target mappings
     * @return difference map
     */
    private static MapDifference<String, Object> calculateMapDifference(
        Map<String, Object> currentMappings, Map<String, Object> targetMappings) {
      // Identify dynamic fields in target (fields with dynamic=true) - recursively search all
      // levels
      Set<String> targetDynamicFields = findDynamicFields(targetMappings, "");

      if (!targetDynamicFields.isEmpty()) {
        log.info("Dynamic fields filtered from comparison: {}", targetDynamicFields);
        Map<String, Object> filteredCurrentMappings =
            removeDynamicFields(currentMappings, targetDynamicFields);
        Map<String, Object> filteredTargetMappings =
            removeDynamicFields(targetMappings, targetDynamicFields);

        MapDifference<String, Object> diff =
            Maps.difference(filteredCurrentMappings, filteredTargetMappings);
        log.info(
            "Map difference - entriesOnlyOnLeft: {}, entriesOnlyOnRight: {}, entriesDiffering: {}",
            diff.entriesOnlyOnLeft().keySet(),
            diff.entriesOnlyOnRight().keySet(),
            diff.entriesDiffering().keySet());
        return diff;
      }

      MapDifference<String, Object> diff = Maps.difference(currentMappings, targetMappings);
      log.info(
          "No dynamic fields found. Map difference - entriesOnlyOnLeft: {}, entriesOnlyOnRight: {}, entriesDiffering: {}",
          diff.entriesOnlyOnLeft().keySet(),
          diff.entriesOnlyOnRight().keySet(),
          diff.entriesDiffering().keySet());
      return diff;
    }

    /**
     * Recursively find all dynamic fields in the mappings
     *
     * @param mappings the mappings to search
     * @param prefix the current path prefix
     * @return set of field paths that are dynamic
     */
    private static Set<String> findDynamicFields(Map<String, Object> mappings, String prefix) {
      Set<String> dynamicFields = new HashSet<>();

      for (Map.Entry<String, Object> entry : mappings.entrySet()) {
        String fieldPath = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();

        if (entry.getValue() instanceof Map) {
          Map<String, Object> fieldMapping = (Map<String, Object>) entry.getValue();

          // Check if this field is explicitly marked as dynamic
          if (fieldMapping.containsKey("dynamic")
              && (Boolean.TRUE.equals(fieldMapping.get("dynamic"))
                  || "true".equals(fieldMapping.get("dynamic")))) {
            dynamicFields.add(fieldPath);
          }

          // Check if this field is known to be dynamic based on field name patterns
          // This handles cases where ES drops dynamic=true from the mapping but the field should be
          // dynamic
          if (isKnownDynamicField(entry.getKey(), fieldMapping)) {
            dynamicFields.add(fieldPath);
          }

          // Recursively search in properties if they exist
          if (fieldMapping.containsKey("properties")
              && fieldMapping.get("properties") instanceof Map) {
            Map<String, Object> properties = (Map<String, Object>) fieldMapping.get("properties");
            String propertiesPath = fieldPath.isEmpty() ? "properties" : fieldPath + ".properties";
            dynamicFields.addAll(findDynamicFields(properties, propertiesPath));
          }
        }
      }

      return dynamicFields;
    }

    /**
     * Check if a field is known to be dynamic based on its name and mapping structure. This handles
     * cases where Elasticsearch drops dynamic=true from the mapping but the field should be
     * dynamic.
     *
     * @param fieldName the field name
     * @param fieldMapping the field mapping
     * @return true if the field is known to be dynamic
     */
    private static boolean isKnownDynamicField(String fieldName, Map<String, Object> fieldMapping) {
      // ownerTypes is dynamically built based on ownership types present in data
      if ("ownerTypes".equals(fieldName) && "object".equals(fieldMapping.get("type"))) {
        return true;
      }

      // structuredProperties is dynamic by design
      if ("structuredProperties".equals(fieldName) && "object".equals(fieldMapping.get("type"))) {
        return true;
      }

      return false;
    }

    /**
     * Remove dynamic fields from mappings based on field paths
     *
     * @param mappings the mappings to filter
     * @param dynamicFieldPaths the paths of dynamic fields to remove
     * @return filtered mappings
     */
    private static Map<String, Object> removeDynamicFields(
        Map<String, Object> mappings, Set<String> dynamicFieldPaths) {
      Map<String, Object> filtered = new HashMap<>(mappings);

      for (String fieldPath : dynamicFieldPaths) {
        removeDynamicField(filtered, fieldPath);
      }

      return filtered;
    }

    /**
     * Remove a single dynamic field from mappings based on its path
     *
     * @param mappings the mappings to modify
     * @param fieldPath the path of the field to remove (e.g., "_aspects.ownership.ownerTypes")
     */
    private static void removeDynamicField(Map<String, Object> mappings, String fieldPath) {
      String[] pathParts = fieldPath.split("\\.");

      if (pathParts.length == 1) {
        // Top-level field
        mappings.remove(pathParts[0]);
      } else {
        // Nested field - navigate to the parent and remove the field
        Map<String, Object> current = mappings;
        for (int i = 0; i < pathParts.length - 1; i++) {
          Object next = current.get(pathParts[i]);
          if (next instanceof Map) {
            current = (Map<String, Object>) next;
          } else {
            return; // Path doesn't exist
          }
        }
        current.remove(pathParts[pathParts.length - 1]);
      }
    }

    /**
     * Given a mapping return a unique string for each version/typed structured property
     *
     * @param mappings Elastic mappings
     * @return set of unique ids for each versioned/typed structured property
     */
    private static Set<String> getVersionedStructuredPropertyIds(Map<String, Object> mappings) {
      Map<String, Object> versionedMappings =
          getOrDefault(
              mappings,
              List.of(
                  "properties",
                  STRUCTURED_PROPERTY_MAPPING_FIELD,
                  "properties",
                  STRUCTURED_PROPERTY_MAPPING_VERSIONED_FIELD,
                  "properties"));

      return flattenStructuredPropertyPath(
              Map.entry(STRUCTURED_PROPERTY_MAPPING_VERSIONED_FIELD, versionedMappings), 0)
          .map(Map.Entry::getKey)
          .collect(Collectors.toSet());
    }
  }

  private static Map<String, Object> removeKeys(
      Map<String, Object> mapObject, Set<String> keysToRemove) {
    return mapObject.entrySet().stream()
        .filter(entry -> !keysToRemove.contains(entry.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static boolean equalsGroup(Map<String, Object> newSettings, Settings oldSettings) {
    if (!newSettings.keySet().equals(oldSettings.names())) {
      return false;
    }

    for (String key : newSettings.keySet()) {
      // Skip urn stop filter, as adding new entities will cause this filter to change
      // No need to reindex every time a new entity is added
      if (key.equals("urn_stop_filter")) {
        continue;
      }
      if (newSettings.get(key) instanceof Map) {
        if (!equalsGroup(
            (Map<String, Object>) newSettings.get(key), oldSettings.getByPrefix(key + "."))) {
          return false;
        }
      } else if (newSettings.get(key) instanceof List) {
        if (!newSettings.get(key).equals(oldSettings.getAsList(key))) {
          return false;
        }
      } else {
        String oldValue = oldSettings.get(key);
        Object newValue = newSettings.get(key);
        // Handle null values properly
        if (newValue == null && oldValue == null) {
          continue;
        }
        if (newValue == null || oldValue == null) {
          return false;
        }
        if (!newValue.toString().equals(oldValue)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Return a map with dot delimited path as keys
   *
   * @param entry for root map
   * @return dot delimited key path map
   */
  private static Stream<Map.Entry<String, Object>> flattenStructuredPropertyPath(
      Map.Entry<String, Object> entry, int depth) {
    if (entry.getValue() instanceof Map<?, ?> && depth < 5) {
      Map<String, Object> nested = (Map<String, Object>) entry.getValue();

      return nested.entrySet().stream()
          .map(e -> new AbstractMap.SimpleEntry(entry.getKey() + "." + e.getKey(), e.getValue()))
          .flatMap(e -> flattenStructuredPropertyPath(e, depth + 1));
    }
    return Stream.of(entry);
  }
}
