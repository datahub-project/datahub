package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.UsageEventIndexUtils;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class CreateUsageEventIndicesStep implements UpgradeStep {
  private final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents;
  private final ConfigurationProvider configurationProvider;

  @Override
  public String id() {
    return "CreateUsageEventIndicesStep";
  }

  @Override
  public int retryCount() {
    return 3;
  }

  @Override
  public boolean skip(UpgradeContext context) {
    boolean analyticsEnabled = configurationProvider.getPlatformAnalytics().isEnabled();
    if (!analyticsEnabled) {
      log.info("DataHub analytics is disabled, skipping usage event index setup");
    }
    return !analyticsEnabled;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {

        final String indexPrefix =
            configurationProvider.getElasticSearch().getIndex().getFinalPrefix();

        boolean useOpenSearch = esComponents.getSearchClient().getEngineType().isOpenSearch();
        int numShards = esComponents.getIndexBuilder().getNumShards();
        int numReplicas = esComponents.getIndexBuilder().getNumReplicas();

        if (useOpenSearch) {
          setupOpenSearchUsageEvents(indexPrefix, numShards, numReplicas, context.opContext());
        } else {
          setupElasticsearchUsageEvents(indexPrefix, numShards, numReplicas);
        }

        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("CreateUsageEventIndicesStep failed.", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private void setupElasticsearchUsageEvents(String prefix, int numShards, int numReplicas)
      throws Exception {
    String prefixedPolicy = prefix + "datahub_usage_event_policy";
    String prefixedTemplate = prefix + "datahub_usage_event_index_template";
    String prefixedDataStream = prefix + "datahub_usage_event";

    // Create ILM policy
    UsageEventIndexUtils.createIlmPolicy(esComponents, prefixedPolicy);

    // Create index template
    UsageEventIndexUtils.createIndexTemplate(
        esComponents, prefixedTemplate, prefixedPolicy, numShards, numReplicas, prefix);

    // Create data stream
    UsageEventIndexUtils.createDataStream(esComponents, prefixedDataStream);
  }

  private void setupOpenSearchUsageEvents(
      String prefix, int numShards, int numReplicas, OperationContext operationContext)
      throws Exception {
    String prefixedPolicy = prefix + "datahub_usage_event_policy";
    String prefixedTemplate = prefix + "datahub_usage_event_index_template";
    String prefixedIndex = prefix + "datahub_usage_event-000001";

    // Create ISM policy (both AWS and self-hosted OpenSearch use the same format)
    boolean policyCreated =
        UsageEventIndexUtils.createIsmPolicy(
            esComponents, prefixedPolicy, prefix, operationContext);
    log.info("ISM policy creation result: {}", policyCreated);

    if (policyCreated) {
      log.info("ISM policy created successfully, proceeding with template and index creation");

      // Create index template (both AWS and self-hosted OpenSearch use the same format and
      // endpoint)
      log.info("Creating index template: {}", prefixedTemplate);
      UsageEventIndexUtils.createOpenSearchIndexTemplate(
          esComponents, prefixedTemplate, numShards, numReplicas, prefix);

      // Create initial numbered index (both AWS and self-hosted OpenSearch use the same approach)
      log.info("Creating initial index: {}", prefixedIndex);
      UsageEventIndexUtils.createOpenSearchIndex(esComponents, prefixedIndex, prefix);
    } else {
      log.warn(
          "ISM policy creation failed or is not supported. Skipping template and index creation to avoid configuration issues.");
      log.info("Usage event tracking will not be available without proper policy configuration.");
    }
  }
}
