package com.linkedin.datahub.upgrade.system.entities;

import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.QUERY_ENTITY_NAME;
import static com.linkedin.metadata.graph.elastic.ESGraphQueryDAO.RELATIONSHIP_TYPE;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.config.search.BulkDeleteConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;

@Slf4j
public class RemoveQueryEdges implements NonBlockingSystemUpgrade {
  private final List<UpgradeStep> steps;

  public RemoveQueryEdges(
      OperationContext opContext,
      EntityService<?> entityService,
      ESWriteDAO esWriteDAO,
      boolean enabled,
      BulkDeleteConfiguration deleteConfig) {
    if (enabled) {
      steps =
          ImmutableList.of(
              new RemoveQueryEdgesStep(opContext, esWriteDAO, entityService, deleteConfig));
    } else {
      steps = ImmutableList.of();
    }
  }

  @Override
  public String id() {
    return "RemoveQueryEdges";
  }

  @Override
  public List<UpgradeStep> steps() {
    return steps;
  }

  public static class RemoveQueryEdgesStep implements UpgradeStep {
    private static final String UPGRADE_ID = "RemoveQueryEdges_V1";
    private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);

    private final OperationContext opContext;
    private final EntityService<?> entityService;
    private final ESWriteDAO esWriteDAO;
    private final BulkDeleteConfiguration deleteConfig;

    public RemoveQueryEdgesStep(
        OperationContext opContext,
        ESWriteDAO esWriteDAO,
        EntityService<?> entityService,
        BulkDeleteConfiguration deleteConfig) {
      this.opContext = opContext;
      this.esWriteDAO = esWriteDAO;
      this.entityService = entityService;
      this.deleteConfig = deleteConfig;
    }

    @Override
    public String id() {
      return UPGRADE_ID;
    }

    @Override
    public Function<UpgradeContext, UpgradeStepResult> executable() {
      final String indexName =
          opContext
              .getSearchContext()
              .getIndexConvention()
              .getIndexName(ElasticSearchGraphService.INDEX_NAME);

      return (context) -> {
        BoolQueryBuilder deleteQuery = QueryBuilders.boolQuery();
        deleteQuery.filter(QueryBuilders.termQuery(RELATIONSHIP_TYPE, "IsAssociatedWith"));
        deleteQuery.filter(QueryBuilders.termQuery("source.entityType", QUERY_ENTITY_NAME));

        try {
          esWriteDAO.deleteByQuerySync(indexName, deleteQuery, deleteConfig);
          BootstrapStep.setUpgradeResult(context.opContext(), UPGRADE_ID_URN, entityService);
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
        } catch (Exception e) {
          log.error("Failed to execute query edge delete.", e);
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }
      };
    }

    /**
     * Returns whether the upgrade should proceed if the step fails after exceeding the maximum
     * retries.
     */
    @Override
    public boolean isOptional() {
      return true;
    }

    /**
     * Returns whether the upgrade should be skipped. Uses previous run history or the environment
     * variables REPROCESS_DEFAULT_POLICY_FIELDS & BACKFILL_BROWSE_PATHS_V2 to determine whether to
     * skip.
     */
    @Override
    public boolean skip(UpgradeContext context) {

      boolean previouslyRun =
          entityService.exists(
              context.opContext(), UPGRADE_ID_URN, DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, true);
      if (previouslyRun) {
        log.info("{} was already run. Skipping.", id());
      }
      return previouslyRun;
    }
  }
}
