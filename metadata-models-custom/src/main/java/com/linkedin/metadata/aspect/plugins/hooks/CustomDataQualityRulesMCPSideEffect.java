package com.linkedin.metadata.aspect.plugins.hooks;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import java.util.Collection;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

public class CustomDataQualityRulesMCPSideEffect extends MCPSideEffect {

  public CustomDataQualityRulesMCPSideEffect(AspectPluginConfig aspectPluginConfig) {
    super(aspectPluginConfig);
  }

  @Override
  protected Stream<ChangeMCP> applyMCPSideEffect(
      Collection<ChangeMCP> changeMCPS, @Nonnull AspectRetriever aspectRetriever) {
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
                  .build(aspectRetriever);
            });
  }
}
