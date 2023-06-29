package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.timeseries.BatchWriteOperationsOptions;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.index.query.QueryBuilder;


@RequiredArgsConstructor
@Slf4j
public class EntityIndexBuilders implements ElasticSearchIndexed {
  private final ESIndexBuilder indexBuilder;
  private final EntityRegistry entityRegistry;
  private final IndexConvention indexConvention;
  private final SettingsBuilder settingsBuilder;

  @Override
  public void reindexAll() {
      for (ReindexConfig config : getReindexConfigs()) {
          try {
              indexBuilder.buildIndex(config);
          } catch (IOException e) {
              throw new RuntimeException(e);
          }
      }
  }

  @Override
  public String reindexAsync(String index, @Nullable QueryBuilder filterQuery, BatchWriteOperationsOptions options)
      throws Exception {
    return indexBuilder.reindexInPlaceAsync(index, filterQuery, options);
  }

  @Override
  public List<ReindexConfig> getReindexConfigs() {
    return entityRegistry.getEntitySpecs().values().stream().flatMap(entitySpec -> {
                      try {
                        return new EntityIndexBuilder(indexBuilder, entitySpec, settingsBuilder, indexConvention.getIndexName(entitySpec))
                                .getReindexConfigs().stream();
                      } catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                    }
            ).collect(Collectors.toList());
  }
}
