package com.linkedin.datahub.upgrade.restorebackup;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.commonsteps.GMSQualificationStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.ebean.EbeanServer;
import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.client.RestHighLevelClient;


public class RestoreBackup implements Upgrade {

  private final List<UpgradeStep> _steps;

  public RestoreBackup(final EbeanServer server, final EntityService entityService, final EntityRegistry entityRegistry,
      final GraphService graphClient, final RestHighLevelClient searchClient) {
    _steps = buildSteps(server, entityService, entityRegistry, graphClient, searchClient);
  }

  @Override
  public String id() {
    return "RestoreBackup";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }

  private List<UpgradeStep> buildSteps(final EbeanServer server, final EntityService entityService,
      final EntityRegistry entityRegistry, final GraphService graphClient, final RestHighLevelClient searchClient) {
    final List<UpgradeStep> steps = new ArrayList<>();
    steps.add(new GMSQualificationStep());
    steps.add(new DeleteSearchIndicesStep(searchClient));
    steps.add(new DeleteGraphRelationshipsStep(graphClient));
    steps.add(new ClearAspectV2TableStep(server));
    steps.add(new RestoreStorageStep(entityService, entityRegistry));
    return steps;
  }

  @Override
  public List<UpgradeCleanupStep> cleanupSteps() {
    return ImmutableList.of();
  }
}
