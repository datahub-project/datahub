package com.linkedin.datahub.upgrade.buildindices;

import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.mxe.BuildIndicesHistoryEvent;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.RequestOptions;

import static com.linkedin.datahub.upgrade.buildindices.IndexUtils.*;


@RequiredArgsConstructor
@Slf4j
public class PostBuildIndicesStep implements UpgradeStep {

  private final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents _esComponents;
  private final List<ElasticSearchIndexed> _services;
  private final KafkaEventProducer _kafkaEventProducer;
  private final GitVersion _gitVersion;

  @Override
  public String id() {
    return "PostBuildIndicesStep";
  }

  @Override
  public int retryCount() {
    return 2;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {

        List<ReindexConfig> indexConfigs = getAllReindexConfigs(_services)
                .stream().filter(ReindexConfig::requiresReindex)
                .collect(Collectors.toList());

        // Reset write blocking
        for (ReindexConfig indexConfig : indexConfigs) {
          UpdateSettingsRequest request = new UpdateSettingsRequest(indexConfig.name());
          Map<String, Object> indexSettings = ImmutableMap.of("index.blocks.write", "false");

          request.settings(indexSettings);
          boolean ack =
              _esComponents.getSearchClient().indices().putSettings(request, RequestOptions.DEFAULT).isAcknowledged();
          log.info("Updated index {} with new settings. Settings: {}, Acknowledged: {}", indexConfig.name(), indexSettings, ack);
          if (!ack) {
            log.error(
                "Partial index settings update, some indices may still be blocking writes."
                    + " Please fix the error and rerun the BuildIndices upgrade job.");
            return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
          }
        }

        BuildIndicesHistoryEvent buildIndicesHistoryEvent = new BuildIndicesHistoryEvent()
            .setVersion(_gitVersion.getVersion());
        _kafkaEventProducer.produceBuildIndicesHistoryEvent(buildIndicesHistoryEvent);
      } catch (Exception e) {
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }
}
