package com.linkedin.datahub.upgrade.system.elasticsearch;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.loadindices.AspectIndexLoader;
import com.linkedin.datahub.upgrade.shared.ElasticSearchUpgradeUtils;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.BackfillEntityIndicesStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.IncrementalReindexCatchUpStep;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Ordered post–Phase-1 work for incremental reindex. Exposed as a single non-blocking upgrade so
 * steps always run in sequence.
 */
@Slf4j
public class IncrementalReindex implements NonBlockingSystemUpgrade {

  public static final String ID = "IncrementalReindex";

  private final List<UpgradeStep> steps;

  public IncrementalReindex(
      SystemMetadataService systemMetadataService,
      TimeseriesAspectService timeseriesAspectService,
      EntitySearchService entitySearchService,
      GraphService graphService,
      ConfigurationProvider configurationProvider,
      AspectDao aspectDao,
      OperationContext opContext,
      EntityService<?> entityService,
      String upgradeVersion,
      @Nullable AspectIndexLoader aspectIndexLoader,
      UpdateIndicesService updateIndicesService) {

    Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties;
    if (configurationProvider.getStructuredProperties().isSystemUpdateEnabled()) {
      structuredProperties =
          ElasticSearchUpgradeUtils.getActiveStructuredPropertiesDefinitions(aspectDao);
    } else {
      structuredProperties = Set.of();
    }

    BuildIndicesConfiguration buildIndicesConfig =
        configurationProvider.getElasticSearch().getBuildIndices();

    List<ElasticSearchIndexed> indexedServices =
        ElasticSearchUpgradeUtils.createElasticSearchIndexedServices(
            graphService, entitySearchService, systemMetadataService, timeseriesAspectService);

    steps = new ArrayList<>();
    // Phase 1 plans fills only where this loader exists, so its absence means there is nothing to
    // fill in this deployment, not a fill left behind.
    if (aspectIndexLoader != null) {
      steps.add(
          new BackfillEntityIndicesStep(
              opContext,
              entityService,
              indexedServices,
              structuredProperties,
              upgradeVersion,
              buildIndicesConfig,
              aspectIndexLoader,
              updateIndicesService));
    } else {
      log.info("Primary storage cannot be streamed for index fills here; fill step not added");
    }
    steps.add(
        new IncrementalReindexCatchUpStep(
            opContext,
            entityService,
            aspectDao,
            indexedServices,
            structuredProperties,
            upgradeVersion,
            buildIndicesConfig));
  }

  @Override
  public String id() {
    return ID;
  }

  @Override
  public List<UpgradeStep> steps() {
    return steps;
  }
}
