/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.system.restoreindices.graph.vianodes;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.system.AbstractMCLStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;

@Slf4j
public class ReindexDataJobViaNodesCLLStep extends AbstractMCLStep {

  public ReindexDataJobViaNodesCLLStep(
      OperationContext opContext,
      EntityService<?> entityService,
      AspectDao aspectDao,
      Integer batchSize,
      Integer batchDelayMs,
      Integer limit) {
    super(opContext, entityService, aspectDao, batchSize, batchDelayMs, limit);
  }

  @Override
  public String id() {
    return "via-node-cll-reindex-datajob-v3";
  }

  @Nonnull
  @Override
  protected String getAspectName() {
    return DATA_JOB_INPUT_OUTPUT_ASPECT_NAME;
  }

  @Nullable
  @Override
  protected String getUrnLike() {
    return "urn:li:" + DATA_JOB_ENTITY_NAME + ":%";
  }

  @Override
  /**
   * Returns whether the upgrade should be skipped. Uses previous run history or the environment
   * variable SKIP_REINDEX_DATA_JOB_INPUT_OUTPUT to determine whether to skip.
   */
  public boolean skip(UpgradeContext context) {
    boolean envFlagRecommendsSkip =
        Boolean.parseBoolean(System.getenv("SKIP_REINDEX_DATA_JOB_INPUT_OUTPUT"));
    if (envFlagRecommendsSkip) {
      log.info("Environment variable SKIP_REINDEX_DATA_JOB_INPUT_OUTPUT is set to true. Skipping.");
    }
    return (super.skip(context) || envFlagRecommendsSkip);
  }
}
