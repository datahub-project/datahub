package com.linkedin.datahub.upgrade.usageevents;

import com.linkedin.datahub.upgrade.system.elasticsearch.util.UsageEventIndexUtils;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.datahubusage.UsageEventsInfrastructureProvisioner;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Provisions OpenSearch/Elasticsearch usage-event indices (ILM/ISM policies, templates, data stream
 * or initial index).
 */
@RequiredArgsConstructor
@Slf4j
public class ElasticsearchUsageEventsInfrastructureProvisioner
    implements UsageEventsInfrastructureProvisioner {

  private final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents;
  private final ConfigurationProvider configurationProvider;

  @Override
  public void provision(@Nonnull OperationContext opContext) throws Exception {
    final String indexPrefix = configurationProvider.getElasticSearch().getIndex().getFinalPrefix();

    boolean useOpenSearch = esComponents.getSearchClient().getEngineType().isOpenSearch();
    int numShards = configurationProvider.getElasticSearch().getIndex().getNumShards();
    int numReplicas = configurationProvider.getElasticSearch().getIndex().getNumReplicas();

    if (useOpenSearch) {
      setupOpenSearchUsageEvents(indexPrefix, numShards, numReplicas, opContext);
    } else {
      setupElasticsearchUsageEvents(indexPrefix, numShards, numReplicas, opContext);
    }
  }

  private void setupElasticsearchUsageEvents(
      String prefix, int numShards, int numReplicas, OperationContext opContext) throws Exception {
    String prefixedPolicy = prefix + "datahub_usage_event_policy";
    String prefixedTemplate = prefix + "datahub_usage_event_index_template";
    String prefixedDataStream = prefix + "datahub_usage_event";

    UsageEventIndexUtils.createIlmPolicy(opContext, esComponents, prefixedPolicy);

    UsageEventIndexUtils.createIndexTemplate(
        opContext, esComponents, prefixedTemplate, prefixedPolicy, numShards, numReplicas, prefix);

    UsageEventIndexUtils.createDataStream(opContext, esComponents, prefixedDataStream);
  }

  private void setupOpenSearchUsageEvents(
      String prefix, int numShards, int numReplicas, OperationContext operationContext)
      throws Exception {
    String prefixedPolicy = prefix + "datahub_usage_event_policy";
    String prefixedTemplate = prefix + "datahub_usage_event_index_template";
    String prefixedAlias = prefix + "datahub_usage_event";
    String prefixedIndex = prefix + "datahub_usage_event-000001";

    boolean policyCreated =
        UsageEventIndexUtils.createIsmPolicy(
            esComponents, prefixedPolicy, prefix, operationContext);
    log.info("ISM policy creation result: {}", policyCreated);

    if (policyCreated) {
      log.info("ISM policy created successfully, proceeding with template and index creation");

      log.info("Creating index template: {}", prefixedTemplate);
      UsageEventIndexUtils.createOpenSearchIndexTemplate(
          operationContext, esComponents, prefixedTemplate, numShards, numReplicas, prefix);

      log.info("Creating initial index: {} with alias: {}", prefixedIndex, prefixedAlias);
      UsageEventIndexUtils.createOpenSearchUsageEventIndex(
          operationContext, esComponents, prefixedIndex, prefixedAlias);
    } else {
      log.warn(
          "ISM policy creation failed or is not supported. Skipping template and index creation to avoid configuration issues.");
      log.info("Usage event tracking will not be available without proper policy configuration.");
    }
  }
}
