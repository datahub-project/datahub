/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.restorebackup;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.common.steps.ClearGraphServiceStep;
import com.linkedin.datahub.upgrade.common.steps.ClearSearchServiceStep;
import com.linkedin.datahub.upgrade.common.steps.ClearSystemMetadataServiceStep;
import com.linkedin.datahub.upgrade.common.steps.GMSDisableWriteModeStep;
import com.linkedin.datahub.upgrade.common.steps.GMSEnableWriteModeStep;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import io.ebean.Database;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

public class RestoreBackup implements Upgrade {

  private final List<UpgradeStep> _steps;

  public RestoreBackup(
      @Nullable final Database server,
      final EntityService<?> entityService,
      final EntityRegistry entityRegistry,
      final SystemEntityClient systemEntityClient,
      final SystemMetadataService systemMetadataService,
      final EntitySearchService entitySearchService,
      final GraphService graphClient) {
    if (server != null) {
      _steps =
          buildSteps(
              server,
              entityService,
              entityRegistry,
              systemEntityClient,
              systemMetadataService,
              entitySearchService,
              graphClient);
    } else {
      _steps = List.of();
    }
  }

  @Override
  public String id() {
    return "RestoreBackup";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }

  private List<UpgradeStep> buildSteps(
      final Database server,
      final EntityService<?> entityService,
      final EntityRegistry entityRegistry,
      final SystemEntityClient systemEntityClient,
      final SystemMetadataService systemMetadataService,
      final EntitySearchService entitySearchService,
      final GraphService graphClient) {
    final List<UpgradeStep> steps = new ArrayList<>();
    steps.add(new GMSDisableWriteModeStep(systemEntityClient));
    steps.add(new ClearSystemMetadataServiceStep(systemMetadataService, true));
    steps.add(new ClearSearchServiceStep(entitySearchService, true));
    steps.add(new ClearGraphServiceStep(graphClient, true));
    steps.add(new ClearAspectV2TableStep(server));
    steps.add(new RestoreStorageStep(entityService, entityRegistry));
    steps.add(new GMSEnableWriteModeStep(systemEntityClient));
    return steps;
  }

  @Override
  public List<UpgradeCleanupStep> cleanupSteps() {
    return ImmutableList.of();
  }
}
