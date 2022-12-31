package com.linkedin.datahub.upgrade.buildindices;

import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.ResizeRequest;

import static com.linkedin.datahub.upgrade.buildindices.IndexUtils.*;


@RequiredArgsConstructor
@Slf4j
public class PreConfigureESStep implements UpgradeStep {

  private final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents _esComponents;
  private final List<ElasticSearchIndexed> _services;
  private final ConfigurationProvider _configurationProvider;

  @Override
  public String id() {
    return "PreConfigureESStep";
  }

  @Override
  public int retryCount() {
    return 2;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        // Get indices to update
        List<ReindexConfig> indexConfigs = getAllReindexConfigs(_services)
                .stream().filter(ReindexConfig::requiresReindex)
                .collect(Collectors.toList());

        for (ReindexConfig indexConfig : indexConfigs) {
          UpdateSettingsRequest request = new UpdateSettingsRequest(indexConfig.name());
          Map<String, Object> indexSettings = ImmutableMap.of("index.blocks.write", "true");

          request.settings(indexSettings);
          boolean ack;
          try {
            ack =
                _esComponents.getSearchClient().indices().putSettings(request, RequestOptions.DEFAULT).isAcknowledged();
          } catch (ElasticsearchStatusException ese) {
            // Cover first run case, indices won't exist so settings updates won't work nor will the rest of the preConfigure steps.
            // Since no data are in there they are skippable.
            // Have to hack around HighLevelClient not sending the actual Java type nor having an easy way to extract it :(
            if (ese.getMessage().contains("index_not_found")) {
              continue;
            }
            throw ese;
          }
          log.info("Updated index {} with new settings. Settings: {}, Acknowledged: {}", indexConfig.name(), indexSettings, ack);
          if (!ack) {
            log.error("Partial index settings update, some indices may still be blocking writes."
                + " Please fix the error and re-run the BuildIndices upgrade job.");
            return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
          }

          // Clone indices
          if (_configurationProvider.getElasticSearch().getBuildIndices().isCloneIndices()) {
            String clonedName = indexConfig.name() + "_clone_" + System.currentTimeMillis();
            ResizeRequest resizeRequest = new ResizeRequest(clonedName, indexConfig.name());
            boolean cloneAck =
                    _esComponents.getSearchClient().indices().clone(resizeRequest, RequestOptions.DEFAULT).isAcknowledged();
            log.info("Cloned index {} into {}, Acknowledged: {}", indexConfig.name(), clonedName, cloneAck);
            if (!cloneAck) {
              log.error("Partial index settings update, cloned indices may need to be cleaned up: {}", clonedName);
              return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
            }
          }
        }
      } catch (Exception e) {
        log.error("PreConfigureESStep failed.", e);
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }
}
