package com.linkedin.datahub.upgrade.system.dataprocessinstances;

import static com.linkedin.metadata.Constants.*;

import com.google.common.base.Throwables;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

@Slf4j
public class BackfillDataProcessInstancesHasRunEventsStep implements UpgradeStep {

  private static final String UPGRADE_ID = "BackfillDataProcessInstancesHasRunEvents";
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final ElasticSearchService elasticSearchService;
  private final RestHighLevelClient restHighLevelClient;

  private final boolean reprocessEnabled;
  private final Integer batchSize;

  public BackfillDataProcessInstancesHasRunEventsStep(
      OperationContext opContext,
      EntityService<?> entityService,
      ElasticSearchService elasticSearchService,
      RestHighLevelClient restHighLevelClient,
      boolean reprocessEnabled,
      Integer batchSize) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.elasticSearchService = elasticSearchService;
    this.restHighLevelClient = restHighLevelClient;
    this.reprocessEnabled = reprocessEnabled;
    this.batchSize = batchSize;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      final AuditStamp auditStamp =
          new AuditStamp()
              .setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR))
              .setTime(System.currentTimeMillis());

      TermsValuesSourceBuilder termsValuesSourceBuilder =
          new TermsValuesSourceBuilder("urn").field("urn");

      CompositeAggregationBuilder aggregationBuilder =
          AggregationBuilders.composite("aggs", List.of(termsValuesSourceBuilder)).size(batchSize);
      ObjectNode json = JsonNodeFactory.instance.objectNode();
      json.put("hasRunEvents", true);

      IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();

      String runEventsIndexName =
          indexConvention.getTimeseriesAspectIndexName(
              DATA_PROCESS_INSTANCE_ENTITY_NAME, DATA_PROCESS_INSTANCE_RUN_EVENT_ASPECT_NAME);

      DataHubUpgradeState upgradeState = DataHubUpgradeState.SUCCEEDED;

      while (true) {
        SearchRequest searchRequest = new SearchRequest(runEventsIndexName);
        searchRequest.source(new SearchSourceBuilder().size(0).aggregation(aggregationBuilder));
        SearchResponse response;

        try {
          response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
          log.error(Throwables.getStackTraceAsString(e));
          log.error("Error querying index {}", runEventsIndexName);
          upgradeState = DataHubUpgradeState.FAILED;
          break;
        }
        List<Aggregation> aggregations = response.getAggregations().asList();
        if (aggregations.isEmpty()) {
          break;
        }
        CompositeAggregation aggregation = (CompositeAggregation) aggregations.get(0);
        Set<Urn> urns = new HashSet<>();
        for (CompositeAggregation.Bucket bucket : aggregation.getBuckets()) {
          for (Object value : bucket.getKey().values()) {
            try {
              urns.add(Urn.createFromString(String.valueOf(value)));
            } catch (URISyntaxException e) {
              log.warn("Ignoring invalid urn {}", value);
            }
          }
        }
        if (!urns.isEmpty()) {
          urns = entityService.exists(opContext, urns);
          urns.forEach(
              urn ->
                  elasticSearchService.upsertDocument(
                      opContext,
                      DATA_PROCESS_INSTANCE_ENTITY_NAME,
                      json.toString(),
                      indexConvention.getEntityDocumentId(urn)));
        }
        if (aggregation.afterKey() == null) {
          break;
        }
        aggregationBuilder.aggregateAfter(aggregation.afterKey());
      }
      BootstrapStep.setUpgradeResult(context.opContext(), UPGRADE_ID_URN, entityService);
      return new DefaultUpgradeStepResult(id(), upgradeState);
    };
  }

  @Override
  public String id() {
    return UPGRADE_ID;
  }

  /**
   * Returns whether the upgrade should proceed if the step fails after exceeding the maximum
   * retries.
   */
  @Override
  public boolean isOptional() {
    return true;
  }

  /** Returns whether the upgrade should be skipped. */
  @Override
  public boolean skip(UpgradeContext context) {
    if (reprocessEnabled) {
      return false;
    }

    boolean previouslyRun =
        entityService.exists(
            context.opContext(), UPGRADE_ID_URN, DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, true);
    if (previouslyRun) {
      log.info("{} was already run. Skipping.", id());
    }
    return previouslyRun;
  }
}
