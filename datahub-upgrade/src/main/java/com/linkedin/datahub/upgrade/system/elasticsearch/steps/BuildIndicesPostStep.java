package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import static com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils.INDEX_BLOCKS_WRITE_SETTING;
import static com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils.getAllReindexConfigs;

import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.client.RequestOptions;

@RequiredArgsConstructor
@Slf4j
public class BuildIndicesPostStep implements UpgradeStep {

  private final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents _esComponents;
  private final List<ElasticSearchIndexed> _services;

  @Override
  public String id() {
    return "BuildIndicesPostStep";
  }

  @Override
  public int retryCount() {
    return 3;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {

        List<ReindexConfig> indexConfigs =
            getAllReindexConfigs(_services).stream()
                .filter(ReindexConfig::requiresReindex)
                .collect(Collectors.toList());

        // Reset write blocking
        for (ReindexConfig indexConfig : indexConfigs) {
          UpdateSettingsRequest request = new UpdateSettingsRequest(indexConfig.name());
          Map<String, Object> indexSettings = ImmutableMap.of(INDEX_BLOCKS_WRITE_SETTING, "false");

          request.settings(indexSettings);
          boolean ack =
              _esComponents
                  .getSearchClient()
                  .indices()
                  .putSettings(request, RequestOptions.DEFAULT)
                  .isAcknowledged();
          log.info(
              "Updated index {} with new settings. Settings: {}, Acknowledged: {}",
              indexConfig.name(),
              indexSettings,
              ack);

          if (ack) {
            ack =
                IndexUtils.validateWriteBlock(
                    _esComponents.getSearchClient(), indexConfig.name(), false);
            log.info(
                "Validated index {} with new settings. Settings: {}, Acknowledged: {}",
                indexConfig.name(),
                indexSettings,
                ack);
          }

          if (!ack) {
            log.error(
                "Partial index settings update, some indices may still be blocking writes."
                    + " Please fix the error and rerun the BuildIndices upgrade job.");
            return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
          }
        }
      } catch (Exception e) {
        log.error("BuildIndicesPostStep failed.", e);
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }
}
