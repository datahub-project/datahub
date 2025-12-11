/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.aspect.plugins.hooks;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import java.util.Collection;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

public class CustomDataQualityRulesMCPSideEffect extends MCPSideEffect {

  private AspectPluginConfig config;

  @Override
  protected Stream<ChangeMCP> applyMCPSideEffect(
      Collection<ChangeMCP> changeMCPS, @Nonnull RetrieverContext retrieverContext) {
    // Mirror aspects to another URN in SQL & Search
    return changeMCPS.stream()
        .map(
            changeMCP -> {
              Urn mirror =
                  UrnUtils.getUrn(changeMCP.getUrn().toString().replace(",PROD)", ",DEV)"));
              return ChangeItemImpl.builder()
                  .urn(mirror)
                  .aspectName(changeMCP.getAspectName())
                  .recordTemplate(changeMCP.getRecordTemplate())
                  .auditStamp(changeMCP.getAuditStamp())
                  .systemMetadata(changeMCP.getSystemMetadata())
                  .build(retrieverContext.getAspectRetriever());
            });
  }

  @Override
  protected Stream<MCPItem> postMCPSideEffect(
      Collection<MCLItem> collection, @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  @Nonnull
  @Override
  public AspectPluginConfig getConfig() {
    return config;
  }

  @Override
  public CustomDataQualityRulesMCPSideEffect setConfig(@Nonnull AspectPluginConfig config) {
    this.config = config;
    return this;
  }
}
