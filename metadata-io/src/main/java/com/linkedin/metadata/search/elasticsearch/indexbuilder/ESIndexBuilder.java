package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.client.tasks.TaskSubmissionResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;


@Slf4j
@RequiredArgsConstructor
public class ESIndexBuilder {

  private final RestHighLevelClient searchClient;
  @Getter
  private final int numShards;

  @Getter
  private final int numReplicas;

  @Getter
  private final int numRetries;

  @Getter
  private final int refreshIntervalSeconds;

  @Getter
  private final Map<String, Map<String, String>> indexSettingOverrides;

  @Getter
  private final boolean enableIndexSettingsReindex;

  private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /*
    Most index settings are default values and populated by Elastic. This list is an include list to determine which
    settings we care about when a difference is present.
  */
  private static final List<String> SETTINGS_DYNAMIC = ImmutableList.of("number_of_replicas", "refresh_interval");
  // These setting require reindex
  private static final List<String> SETTINGS_STATIC = ImmutableList.of("number_of_shards");
  private static final List<String> SETTINGS = Stream.concat(
          SETTINGS_DYNAMIC.stream(), SETTINGS_STATIC.stream()).collect(Collectors.toList());

  public void buildIndex(String indexName, Map<String, Object> mappings, Map<String, Object> settings)
      throws IOException {
    // Check if index exists
    boolean exists = searchClient.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);

    Map<String, Object> baseSettings = new HashMap<>(settings);
    baseSettings.put("number_of_shards", numShards);
    baseSettings.put("number_of_replicas", numReplicas);
    baseSettings.put("refresh_interval", String.format("%ss", refreshIntervalSeconds));
    baseSettings.putAll(indexSettingOverrides.getOrDefault(indexName, Map.of()));
    Map<String, Object> finalSettings = ImmutableMap.of("index", baseSettings);

    // If index doesn't exist, create index
    if (!exists) {
      createIndex(indexName, mappings, finalSettings);
      return;
    }

    Map<String, Object> oldMappings = searchClient.indices()
        .getMapping(new GetMappingsRequest().indices(indexName), RequestOptions.DEFAULT)
        .mappings()
        .values()
        .stream()
        .findFirst()
        .get()
        .getSourceAsMap();

    MapDifference<String, Object> mappingsDiff = Maps.difference(
            (Map<String, Object>) oldMappings.getOrDefault("properties", Map.of()),
            (Map<String, Object>) mappings.getOrDefault("properties", Map.of()));

    Settings oldSettings = searchClient.indices()
        .getSettings(new GetSettingsRequest().indices(indexName), RequestOptions.DEFAULT)
        .getIndexToSettings()
        .valuesIt()
        .next();

    final boolean isAnalysisEqual = isAnalysisEqual(finalSettings, oldSettings);
    final boolean isSettingsEqual = isSettingsEqual(finalSettings, oldSettings);
    final boolean isSettingsReindexRequired = isSettingsReindexRequired(finalSettings, oldSettings);

    // If there are no updates to mappings and settings, return
    if (mappingsDiff.areEqual() && isAnalysisEqual && isSettingsEqual) {
      log.info("No updates to index {}", indexName);
      return;
    }

    // If there are no updates to settings, and there are only pure additions to mappings (no updates to existing fields),
    // there is no need to reindex. Just update mappings
    if (isAnalysisEqual && isPureAddition(mappingsDiff) && isSettingsEqual) {
      log.info("New fields have been added to index {}. Updating index in place. Adding: {}", indexName, mappingsDiff);
      PutMappingRequest request = new PutMappingRequest(indexName).source(mappings);
      searchClient.indices().putMapping(request, RequestOptions.DEFAULT);
      log.info("Updated index {} with new mappings", indexName);
      return;
    }

    if (!mappingsDiff.entriesDiffering().isEmpty()) {
      log.info("There's diff between new mappings (left) and old mappings (right): {}", mappingsDiff);
      reindex(indexName, mappings, finalSettings);
    } else {
      log.info("There's an update to settings");
      if (isSettingsReindexRequired) {
        if (enableIndexSettingsReindex) {
          log.info("There's an update to settings that requires reindexing. Target: {}",
                  OBJECT_MAPPER.writeValueAsString(finalSettings));
          reindex(indexName, mappings, finalSettings);
        } else {
          log.warn("There's an update to settings that requires reindexing, however reindexing is disabled. Existing: {} Target: {}",
                  oldSettings, OBJECT_MAPPER.writeValueAsString(finalSettings));
        }
      }

      /*
        If we allow reindexing, then any setting that doesn't require reindexing is also
        applied above and our equality is out of date. We don't want to apply them again for no reason.
       */
      boolean settingsApplied = isSettingsReindexRequired && enableIndexSettingsReindex;
      if (!isSettingsEqual && !settingsApplied) {
        UpdateSettingsRequest request = new UpdateSettingsRequest(indexName);
        Map<String, Object> indexSettings = ((Map<String, Object>) finalSettings.get("index"))
                .entrySet().stream()
                .filter(e -> SETTINGS_DYNAMIC.contains(e.getKey()))
                .collect(Collectors.toMap(e -> "index." + e.getKey(), Map.Entry::getValue));

        /*
          We might not have any changes that can be applied without reindex. This is the case when a reindex
          is needed due to a setting, but not allowed. We don't want to apply empty settings for no reason.
         */
        if (!indexSettings.isEmpty()) {
          request.settings(indexSettings);
          boolean ack = searchClient.indices().putSettings(request, RequestOptions.DEFAULT).isAcknowledged();
          log.info("Updated index {} with new settings. Settings: {}, Acknowledged: {}", indexName,
                  OBJECT_MAPPER.writeValueAsString(indexSettings), ack);
        }
      }
    }
  }

  private void reindex(String indexName, Map<String, Object> mappings, Map<String, Object> finalSettings)
          throws IOException {
    String tempIndexName = indexName + "_" + System.currentTimeMillis();
    createIndex(tempIndexName, mappings, finalSettings);
    try {
      TaskSubmissionResponse reindexTask;
      reindexTask =
          searchClient.submitReindexTask(new ReindexRequest().setSourceIndices(indexName).setDestIndex(tempIndexName),
              RequestOptions.DEFAULT);

      // wait up to 5 minutes for the task to complete
      long startTime = System.currentTimeMillis();
      long millisToWait60Minutes = 1000 * 60 * 60;
      Boolean reindexTaskCompleted = false;

      while ((System.currentTimeMillis() - startTime) < millisToWait60Minutes) {
        log.info("Reindexing from {} to {} in progress...", indexName, tempIndexName);
        ListTasksRequest request = new ListTasksRequest();
        ListTasksResponse tasks = searchClient.tasks().list(request, RequestOptions.DEFAULT);
        if (tasks.getTasks().stream().noneMatch(task -> task.getTaskId().toString().equals(reindexTask.getTask()))) {
          log.info("Reindexing {} to {} task has completed, will now check if reindex was successful", indexName,
              tempIndexName);
          reindexTaskCompleted = true;
          break;
        }
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          log.info("Trouble sleeping while reindexing {} to {}: Exception {}. Retrying...", indexName, tempIndexName,
              e.toString());
        }
      }
      if (!reindexTaskCompleted) {
        throw new RuntimeException(
            String.format("Reindex from %s to %s failed-- task exceeded 60 minute limit", indexName, tempIndexName));
      }
    } catch (Exception e) {
      log.info("Failed to reindex {} to {}: Exception {}", indexName, tempIndexName, e.toString());
      searchClient.indices().delete(new DeleteIndexRequest().indices(tempIndexName), RequestOptions.DEFAULT);
      throw e;
    }

    // Check whether reindex succeeded by comparing document count
    // There can be some delay between the reindex finishing and count being fully up to date, so try multiple times
    long originalCount = 0;
    long reindexedCount = 0;
    for (int i = 0; i < this.numRetries; i++) {
      // Check if reindex succeeded by comparing document counts
      originalCount = getCount(indexName);
      reindexedCount = getCount(tempIndexName);
      if (originalCount == reindexedCount) {
        break;
      }
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    if (originalCount != reindexedCount) {
      log.info("Post-reindex document count is different, source_doc_count: {} reindex_doc_count: {}", originalCount,
          reindexedCount);
      searchClient.indices().delete(new DeleteIndexRequest().indices(tempIndexName), RequestOptions.DEFAULT);
      throw new RuntimeException(String.format("Reindex from %s to %s failed", indexName, tempIndexName));
    }

    log.info("Reindex from {} to {} succeeded", indexName, tempIndexName);
    String indexNamePattern = indexName + "_*";
    // Check if the original index is aliased or not
    GetAliasesResponse aliasesResponse = searchClient.indices()
        .getAlias(new GetAliasesRequest(indexName).indices(indexNamePattern), RequestOptions.DEFAULT);
    // If not aliased, delete the original index
    if (aliasesResponse.getAliases().isEmpty()) {
      searchClient.indices().delete(new DeleteIndexRequest().indices(indexName), RequestOptions.DEFAULT);
    } else {
      searchClient.indices()
          .delete(new DeleteIndexRequest().indices(aliasesResponse.getAliases().keySet().toArray(new String[0])),
              RequestOptions.DEFAULT);
    }

    // Add alias for the new index
    AliasActions removeAction = AliasActions.remove().alias(indexName).index(indexNamePattern);
    AliasActions addAction = AliasActions.add().alias(indexName).index(tempIndexName);
    searchClient.indices()
        .updateAliases(new IndicesAliasesRequest().addAliasAction(removeAction).addAliasAction(addAction),
            RequestOptions.DEFAULT);
    log.info("Finished setting up {}", indexName);
  }

  private long getCount(@Nonnull String indexName) throws IOException {
    return searchClient.count(new CountRequest(indexName).query(QueryBuilders.matchAllQuery()), RequestOptions.DEFAULT)
        .getCount();
  }

  private void createIndex(String indexName, Map<String, Object> mappings, Map<String, Object> settings)
      throws IOException {
    log.info("Index {} does not exist. Creating", indexName);
    CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
    createIndexRequest.mapping(mappings);
    createIndexRequest.settings(settings);
    searchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    log.info("Created index {}", indexName);
  }

  private static boolean isPureAddition(MapDifference<String, Object> mapDifference) {
    return !mapDifference.areEqual() && mapDifference.entriesDiffering().isEmpty()
        && !mapDifference.entriesOnlyOnRight().isEmpty();
  }

  private static boolean isAnalysisEqual(Map<String, Object> newSettings, Settings oldSettings) {
    if (!newSettings.containsKey("index")) {
      return true;
    }
    Map<String, Object> indexSettings = (Map<String, Object>) newSettings.get("index");
    if (!indexSettings.containsKey("analysis")) {
      return true;
    }
    // Compare analysis section
    Map<String, Object> newAnalysis = (Map<String, Object>) indexSettings.get("analysis");
    Settings oldAnalysis = oldSettings.getByPrefix("index.analysis.");
    return equalsGroup(newAnalysis, oldAnalysis);
  }

  private static boolean isSettingsEqual(Map<String, Object> newSettings, Settings oldSettings) {
    if (!newSettings.containsKey("index")) {
      return true;
    }
    Map<String, Object> indexSettings = (Map<String, Object>) newSettings.get("index");
    return SETTINGS.stream()
            .allMatch(settingKey -> Objects.equals(indexSettings.get(settingKey).toString(), oldSettings.get("index." + settingKey)));
  }

  private static boolean isSettingsReindexRequired(Map<String, Object> newSettings, Settings oldSettings) {
    if (!newSettings.containsKey("index")) {
      return false;
    }
    Map<String, Object> indexSettings = (Map<String, Object>) newSettings.get("index");

    if (SETTINGS_STATIC.stream().anyMatch(settingKey ->
            !Objects.equals(indexSettings.get(settingKey).toString(), oldSettings.get("index." + settingKey)))) {
      return true;
    }

    return indexSettings.containsKey("analysis")
            && !equalsGroup((Map<String, Object>) indexSettings.get("analysis"), oldSettings.getByPrefix("index.analysis."));
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
