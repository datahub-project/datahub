package com.linkedin.datahub.upgrade.nocodecleanup;

import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.ebean.Database;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.opensearch.client.RestHighLevelClient;

public class NoCodeCleanupUpgrade implements Upgrade {

  private final List<UpgradeStep> _steps;
  private final List<UpgradeCleanupStep> _cleanupSteps;

  // Upgrade requires the Database.
  public NoCodeCleanupUpgrade(
      @Nullable final Database server,
      final GraphService graphClient,
      final RestHighLevelClient searchClient,
      final IndexConvention indexConvention) {
    if (server != null) {
      _steps = buildUpgradeSteps(server, graphClient, searchClient, indexConvention);
      _cleanupSteps = buildCleanupSteps();
    } else {
      _steps = List.of();
      _cleanupSteps = List.of();
    }
  }

  @Override
  public String id() {
    return "NoCodeDataMigrationCleanup";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }

  @Override
  public List<UpgradeCleanupStep> cleanupSteps() {
    return _cleanupSteps;
  }

  private List<UpgradeCleanupStep> buildCleanupSteps() {
    return Collections.emptyList();
  }

  private List<UpgradeStep> buildUpgradeSteps(
      final Database server,
      final GraphService graphClient,
      final RestHighLevelClient searchClient,
      final IndexConvention indexConvention) {
    final List<UpgradeStep> steps = new ArrayList<>();
    steps.add(new NoCodeUpgradeQualificationStep(server));
    steps.add(new DeleteAspectTableStep(server));
    steps.add(new DeleteLegacyGraphRelationshipsStep(graphClient));
    steps.add(new DeleteLegacySearchIndicesStep(searchClient, indexConvention));
    return steps;
  }
}
