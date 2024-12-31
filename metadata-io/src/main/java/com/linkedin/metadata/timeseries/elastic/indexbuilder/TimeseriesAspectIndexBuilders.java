package com.linkedin.metadata.timeseries.elastic.indexbuilder;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.timeseries.BatchWriteOperationsOptions;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.index.query.QueryBuilder;

@Slf4j
@RequiredArgsConstructor
public class TimeseriesAspectIndexBuilders implements ElasticSearchIndexed {
  @Nonnull private final ESIndexBuilder indexBuilder;
  @Nonnull private final EntityRegistry entityRegistry;
  @Nonnull private final IndexConvention indexConvention;

  @Override
  public void reindexAll(Collection<Pair<Urn, StructuredPropertyDefinition>> properties) {
    for (ReindexConfig config : buildReindexConfigs(properties)) {
      try {
        indexBuilder.buildIndex(config);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public String reindexAsync(
      String index, @Nullable QueryBuilder filterQuery, BatchWriteOperationsOptions options)
      throws Exception {
    Optional<Pair<String, String>> entityAndAspect = indexConvention.getEntityAndAspectName(index);
    if (entityAndAspect.isEmpty()) {
      throw new IllegalArgumentException("Could not extract entity and aspect from index " + index);
    }
    String entityName = entityAndAspect.get().getFirst();
    String aspectName = entityAndAspect.get().getSecond();
    EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
    for (String aspect : entitySpec.getAspectSpecMap().keySet()) {
      if (aspect.toLowerCase().equals(aspectName)) {
        aspectName = aspect;
        break;
      }
    }
    if (!entitySpec.hasAspect(aspectName)) {
      throw new IllegalArgumentException(
          String.format("Could not find aspect %s of entity %s", aspectName, entityName));
    }
    ReindexConfig config =
        indexBuilder.buildReindexState(
            index,
            MappingsBuilder.getMappings(
                entityRegistry.getEntitySpec(entityName).getAspectSpec(aspectName)),
            Collections.emptyMap());
    return indexBuilder.reindexInPlaceAsync(index, filterQuery, options, config);
  }

  @Override
  public List<ReindexConfig> buildReindexConfigs(
      Collection<Pair<Urn, StructuredPropertyDefinition>> properties) {
    return entityRegistry.getEntitySpecs().values().stream()
        .flatMap(
            entitySpec ->
                entitySpec.getAspectSpecs().stream()
                    .map(aspectSpec -> Pair.of(entitySpec, aspectSpec)))
        .filter(pair -> pair.getSecond().isTimeseries())
        .map(
            pair -> {
              try {
                return indexBuilder.buildReindexState(
                    indexConvention.getTimeseriesAspectIndexName(
                        pair.getFirst().getName(), pair.getSecond().getName()),
                    MappingsBuilder.getMappings(pair.getSecond()),
                    Collections.emptyMap());
              } catch (IOException e) {
                log.error(
                    "Issue while building timeseries field index for entity {} aspect {}",
                    pair.getFirst().getName(),
                    pair.getSecond().getName());
                throw new RuntimeException(e);
              }
            })
        .collect(Collectors.toList());
  }
}
