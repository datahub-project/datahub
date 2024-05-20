package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import static com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils.INDEX_BLOCKS_WRITE_SETTING;
import static com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils.getAllReindexConfigs;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_ENTITY_NAME;

import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.Status;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityAspect;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.ResizeRequest;

@RequiredArgsConstructor
@Slf4j
public class BuildIndicesPreStep implements UpgradeStep {
  private final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents _esComponents;
  private final List<ElasticSearchIndexed> _services;
  private final ConfigurationProvider _configurationProvider;
  private final AspectDao _aspectDao;
  private final EntityRegistry _entityRegistry;

  @Override
  public String id() {
    return "BuildIndicesPreStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        final List<ReindexConfig> reindexConfigs;
        if (_configurationProvider.getStructuredProperties().isSystemUpdateEnabled()) {
          reindexConfigs =
              getAllReindexConfigs(_services, getActiveStructuredPropertiesDefinitions(_aspectDao));
        } else {
          reindexConfigs = getAllReindexConfigs(_services);
        }

        // Get indices to update
        List<ReindexConfig> indexConfigs =
            reindexConfigs.stream()
                .filter(ReindexConfig::requiresReindex)
                .collect(Collectors.toList());

        for (ReindexConfig indexConfig : indexConfigs) {
          String indexName =
              IndexUtils.resolveAlias(_esComponents.getSearchClient(), indexConfig.name());

          boolean ack = blockWrites(indexName);
          if (!ack) {
            log.error(
                "Partial index settings update, some indices may still be blocking writes."
                    + " Please fix the error and re-run the BuildIndices upgrade job.");
            return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
          }

          // Clone indices
          if (_configurationProvider.getElasticSearch().getBuildIndices().isCloneIndices()) {
            String clonedName = indexConfig.name() + "_clone_" + System.currentTimeMillis();
            ResizeRequest resizeRequest = new ResizeRequest(clonedName, indexName);
            boolean cloneAck =
                _esComponents
                    .getSearchClient()
                    .indices()
                    .clone(resizeRequest, RequestOptions.DEFAULT)
                    .isAcknowledged();
            log.info("Cloned index {} into {}, Acknowledged: {}", indexName, clonedName, cloneAck);
            if (!cloneAck) {
              log.error(
                  "Partial index settings update, cloned indices may need to be cleaned up: {}",
                  clonedName);
              return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
            }
          }
        }
      } catch (Exception e) {
        log.error("BuildIndicesPreStep failed.", e);
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }

  private boolean blockWrites(String indexName) throws InterruptedException, IOException {
    UpdateSettingsRequest request = new UpdateSettingsRequest(indexName);
    Map<String, Object> indexSettings = ImmutableMap.of(INDEX_BLOCKS_WRITE_SETTING, "true");

    request.settings(indexSettings);
    boolean ack;
    try {
      ack =
          _esComponents
              .getSearchClient()
              .indices()
              .putSettings(request, RequestOptions.DEFAULT)
              .isAcknowledged();
      log.info(
          "Updated index {} with new settings. Settings: {}, Acknowledged: {}",
          indexName,
          indexSettings,
          ack);
    } catch (OpenSearchStatusException | IOException ese) {
      // Cover first run case, indices won't exist so settings updates won't work nor will the rest
      // of the preConfigure steps.
      // Since no data are in there they are skippable.
      // Have to hack around HighLevelClient not sending the actual Java type nor having an easy way
      // to extract it :(
      if (ese.getMessage().contains("index_not_found")) {
        return true;
      } else {
        throw ese;
      }
    }

    if (ack) {
      ack = IndexUtils.validateWriteBlock(_esComponents.getSearchClient(), indexName, true);
      log.info(
          "Validated index {} with new settings. Settings: {}, Acknowledged: {}",
          indexName,
          indexSettings,
          ack);
    }

    return ack;
  }

  private static Set<StructuredPropertyDefinition> getActiveStructuredPropertiesDefinitions(
      AspectDao aspectDao) {
    Set<String> removedStructuredPropertyUrns;
    try (Stream<EntityAspect> stream =
        aspectDao.streamAspects(STRUCTURED_PROPERTY_ENTITY_NAME, STATUS_ASPECT_NAME)) {
      removedStructuredPropertyUrns =
          stream
              .map(
                  entityAspect ->
                      Pair.of(
                          entityAspect.getUrn(),
                          RecordUtils.toRecordTemplate(Status.class, entityAspect.getMetadata())))
              .filter(status -> status.getSecond().isRemoved())
              .map(Pair::getFirst)
              .collect(Collectors.toSet());
    }

    try (Stream<EntityAspect> stream =
        aspectDao.streamAspects(
            STRUCTURED_PROPERTY_ENTITY_NAME, STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME)) {
      return stream
          .map(
              entityAspect ->
                  Pair.of(
                      entityAspect.getUrn(),
                      RecordUtils.toRecordTemplate(
                          StructuredPropertyDefinition.class, entityAspect.getMetadata())))
          .filter(definition -> !removedStructuredPropertyUrns.contains(definition.getKey()))
          .map(Pair::getSecond)
          .collect(Collectors.toSet());
    }
  }
}
