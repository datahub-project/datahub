package com.linkedin.datahub.upgrade.loadindices;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.shared.ElasticSearchUpgradeUtils;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.BuildIndicesStep;
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
import io.ebean.Database;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

public class LoadIndices implements Upgrade {
  public static final String BATCH_SIZE_ARG_NAME = "batchSize";
  public static final String LIMIT_ARG_NAME = "limit";
  public static final String URN_LIKE_ARG_NAME = "urnLike";
  public static final String GE_PIT_EPOCH_MS_ARG_NAME = "gePitEpochMs";
  public static final String LE_PIT_EPOCH_MS_ARG_NAME = "lePitEpochMs";
  public static final String ASPECT_NAMES_ARG_NAME = "aspectNames";

  private final List<UpgradeStep> _steps;

  public LoadIndices(
      @Nullable final Database server,
      final EntityService<?> entityService,
      final UpdateIndicesService updateIndicesService,
      @Nullable final LoadIndicesIndexManager indexManager,
      @Nullable final SystemMetadataService systemMetadataService,
      @Nullable final TimeseriesAspectService timeseriesAspectService,
      @Nullable final EntitySearchService entitySearchService,
      @Nullable final GraphService graphService,
      @Nullable final AspectDao aspectDao) {
    if (server != null && indexManager != null) {
      _steps =
          buildSteps(
              server,
              entityService,
              updateIndicesService,
              indexManager,
              systemMetadataService,
              timeseriesAspectService,
              entitySearchService,
              graphService,
              aspectDao);
    } else {
      _steps = List.of();
    }
  }

  @Override
  public String id() {
    return "LoadIndices";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }

  private List<UpgradeStep> buildSteps(
      final Database server,
      final EntityService<?> entityService,
      final UpdateIndicesService updateIndicesService,
      final LoadIndicesIndexManager indexManager,
      final SystemMetadataService systemMetadataService,
      final TimeseriesAspectService timeseriesAspectService,
      final EntitySearchService entitySearchService,
      final GraphService graphService,
      final AspectDao aspectDao) {
    final List<UpgradeStep> steps = new ArrayList<>();

    if (systemMetadataService != null
        && timeseriesAspectService != null
        && entitySearchService != null
        && graphService != null
        && aspectDao != null) {

      final Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties =
          ElasticSearchUpgradeUtils.getActiveStructuredPropertiesDefinitions(aspectDao);

      List<ElasticSearchIndexed> indexedServices =
          ElasticSearchUpgradeUtils.createElasticSearchIndexedServices(
              graphService, entitySearchService, systemMetadataService, timeseriesAspectService);

      steps.add(new BuildIndicesStep(indexedServices, structuredProperties));
    }

    steps.add(new LoadIndicesStep(server, entityService, updateIndicesService, indexManager));
    return steps;
  }

  @Override
  public List<UpgradeCleanupStep> cleanupSteps() {
    return ImmutableList.of();
  }
}
