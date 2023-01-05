package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.utils.ESUtils;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.linkedin.metadata.shared.ElasticSearchIndexed;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.tasks.TaskInfo;


@Slf4j
@RequiredArgsConstructor
public class EntityIndexBuilder implements ElasticSearchIndexed {
  private final ESIndexBuilder indexBuilder;
  private final EntitySpec entitySpec;
  private final SettingsBuilder settingsBuilder;
  private final String indexName;

  @Override
  public void reindexAll(List<TaskInfo> taskInfos) throws IOException {
    log.info("Setting up index: {}", indexName);
    for (ReindexConfig config : getReindexConfigs()) {
      Optional<TaskInfo> taskInfo = taskInfos.stream()
          .filter(info ->
              ESUtils.getOpaqueIdHeaderValue(indexBuilder.getGitVersion().getVersion(), config.name())
                  .equals(info.getHeaders().get(ESUtils.OPAQUE_ID_HEADER))).findFirst();
      if (taskInfo.isPresent()) {
        log.info("Reindex task {} in progress with description {}. Attempting to continue task from breakpoint.",
            taskInfo.get().getId(), taskInfo.get().getDescription());
        continue;
      }
      indexBuilder.buildIndex(config);
    }
  }

  @Override
  public List<ReindexConfig> getReindexConfigs() throws IOException {
    Map<String, Object> mappings = MappingsBuilder.getMappings(entitySpec);
    Map<String, Object> settings = settingsBuilder.getSettings();
    return List.of(indexBuilder.buildReindexState(indexName, mappings, settings));
  }
}
