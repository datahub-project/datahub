package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Builder
@Getter
@Accessors(fluent = true)
public class ReindexConfig {
    public final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    /*
      Most index settings are default values and populated by Elastic. This list is an include list to determine which
      settings we care about when a difference is present.
    */
    public static final List<String> SETTINGS_DYNAMIC = ImmutableList.of("number_of_replicas", "refresh_interval");
    // These setting require reindex
    public static final List<String> SETTINGS_STATIC = ImmutableList.of("number_of_shards");
    public static final List<String> SETTINGS = Stream.concat(
            SETTINGS_DYNAMIC.stream(), SETTINGS_STATIC.stream()).collect(Collectors.toList());

    final private String name;
    final private boolean exists;
    final private Settings currentSettings;
    final private Map<String, Object> targetSettings;
    final private Map<String, Object> currentMappings;
    final private Map<String, Object> targetMappings;
    final private boolean enableIndexMappingsReindex;
    final private boolean enableIndexSettingsReindex;
    final private String version;

    /* Calculated */
    final private boolean requiresReindex;
    final private boolean requiresApplySettings;
    final private boolean requiresApplyMappings;
    final private boolean isPureMappingsAddition;
    final private boolean isSettingsReindex;

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
            return input.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                                if (e.getValue() instanceof Map) {
                                    return sortMap((Map<String, Object>) e.getValue());
                                } else {
                                    return String.valueOf(e.getValue());
                                }
                            },
                            (oldValue, newValue) -> newValue, TreeMap::new));
        }
    }

    /**
     * Implement calculated fields
     */
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
                MapDifference<String, Object> mappingsDiff = Maps.difference(
                        (TreeMap<String, Object>) super.currentMappings.getOrDefault("properties", new TreeMap()),
                        (TreeMap<String, Object>) super.targetMappings.getOrDefault("properties", new TreeMap()));
                super.requiresApplyMappings = !mappingsDiff.entriesDiffering().isEmpty()
                        || !mappingsDiff.entriesOnlyOnRight().isEmpty();
                super.isPureMappingsAddition = super.requiresApplyMappings
                        && mappingsDiff.entriesDiffering().isEmpty()
                        && !mappingsDiff.entriesOnlyOnRight().isEmpty();
                if (super.requiresApplyMappings && super.isPureMappingsAddition) {
                    log.info("Index: {} - New fields have been added to index. Adding: {}",
                            super.name, mappingsDiff.entriesOnlyOnRight());
                } else  if (super.requiresApplyMappings) {
                    log.info("Index: {} - There's diff between new mappings (left) and old mappings (right): {}",
                            super.name, mappingsDiff.entriesDiffering());
                }

                /* Consider analysis and settings changes */
                super.requiresApplySettings = !isSettingsEqual() || !isAnalysisEqual();
                super.isSettingsReindex = isSettingsReindexRequired();

                /* Determine reindexing required - some settings and mappings do not require reindex, analysis always does */
                if (super.requiresApplyMappings && !super.isPureMappingsAddition) {
                    if (super.enableIndexMappingsReindex) {
                        super.requiresReindex = true;
                    } else {
                        log.warn("Index: {} - There's diff between new mappings, however reindexing is DISABLED.", super.name);
                    }
                }
                if (super.isSettingsReindex) {
                    try {
                        if (!isAnalysisEqual()) {
                            log.info("Index: {} - There's an update to `analysis` settings that requires reindexing. Target: {} Current: {}",
                                    super.name, OBJECT_MAPPER.writeValueAsString(super.targetSettings), super.currentSettings);
                        }
                        if (!isSettingsEqual()) {
                            log.info("Index: {} - There's an update to settings that requires reindexing. Target: {} Current: {}",
                                    super.name, OBJECT_MAPPER.writeValueAsString(super.targetSettings), super.currentSettings);
                        }
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    if (super.enableIndexSettingsReindex) {
                        super.requiresReindex = true;
                    } else {
                        log.warn("Index: {} - There's an update to settings that requires reindexing, however reindexing is DISABLED", super.name);
                    }
                }
            }
            return super.build();
        }

        private boolean isAnalysisEqual() {
            if (!super.targetSettings.containsKey("index")) {
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
            if (!super.targetSettings.containsKey("index")) {
                return true;
            }
            Map<String, Object> indexSettings = (Map<String, Object>) super.targetSettings.get("index");
            return SETTINGS.stream()
                    .allMatch(settingKey -> Objects.equals(indexSettings.get(settingKey).toString(),
                            super.currentSettings.get("index." + settingKey)));
        }

        private boolean isSettingsReindexRequired() {
            if (!super.targetSettings.containsKey("index")) {
                return false;
            }
            Map<String, Object> indexSettings = (Map<String, Object>) super.targetSettings.get("index");

            if (SETTINGS_STATIC.stream().anyMatch(settingKey ->
                    !Objects.equals(indexSettings.get(settingKey).toString(), super.currentSettings.get("index." + settingKey)))) {
                return true;
            }

            return indexSettings.containsKey("analysis")
                    && !equalsGroup((Map<String, Object>) indexSettings.get("analysis"),
                    super.currentSettings.getByPrefix("index.analysis."));
        }
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
                if (!equalsGroup((Map<String, Object>) newSettings.get(key), oldSettings.getByPrefix(key + "."))) {
                    return false;
                }
            } else if (newSettings.get(key) instanceof List) {
                if (!newSettings.get(key).equals(oldSettings.getAsList(key))) {
                    return false;
                }
            } else {
                if (!newSettings.get(key).toString().equals(oldSettings.get(key))) {
                    return false;
                }
            }
        }
        return true;
    }
}
