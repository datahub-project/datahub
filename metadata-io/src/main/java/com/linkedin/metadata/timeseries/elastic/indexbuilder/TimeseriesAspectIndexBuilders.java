package com.linkedin.metadata.timeseries.elastic.indexbuilder;

import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.linkedin.util.Pair;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.tasks.TaskInfo;


@Slf4j
@RequiredArgsConstructor
public class TimeseriesAspectIndexBuilders implements ElasticSearchIndexed {
  private final ESIndexBuilder _indexBuilder;
  private final EntityRegistry _entityRegistry;
  private final IndexConvention _indexConvention;

  @Override
  public void reindexAll(List<TaskInfo> taskInfos) {
    for (ReindexConfig config : getReindexConfigs()) {
      try {
        Optional<TaskInfo> taskInfo = taskInfos.stream()
            .filter(info ->
                ESUtils.getOpaqueIdHeaderValue(_indexBuilder.getGitVersion().getVersion(), config.name())
                    .equals(info.getHeaders().get(ESUtils.OPAQUE_ID_HEADER))).findFirst();
        if (taskInfo.isPresent()) {
          log.info("Reindex task {} in progress with description {}. Attempting to continue task from breakpoint.",
              taskInfo.get().getId(), taskInfo.get().getDescription());
          continue;
        }
        _indexBuilder.buildIndex(config);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public List<ReindexConfig> getReindexConfigs() {
    return _entityRegistry.getEntitySpecs().values().stream()
            .flatMap(entitySpec -> entitySpec.getAspectSpecs().stream()
                    .map(aspectSpec -> Pair.of(entitySpec, aspectSpec)))
            .filter(pair -> pair.getSecond().isTimeseries())
            .map(pair -> {
              try {
                return _indexBuilder.buildReindexState(
                        _indexConvention.getTimeseriesAspectIndexName(pair.getFirst().getName(), pair.getSecond().getName()),
                        MappingsBuilder.getMappings(pair.getSecond()), Collections.emptyMap());
              } catch (IOException e) {
                log.error("Issue while building timeseries field index for entity {} aspect {}", pair.getFirst().getName(),
                        pair.getSecond().getName());
                throw new RuntimeException(e);
              }
            }).collect(Collectors.toList());
  }
}
