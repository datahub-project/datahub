package com.linkedin.metadata.aspect.plugins.hooks;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.batch.UpsertItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectRetriever;
import com.linkedin.metadata.entity.ebean.batch.MCPUpsertBatchItem;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

public class CustomDataQualityRulesMCPSideEffect extends MCPSideEffect {

  public CustomDataQualityRulesMCPSideEffect(AspectPluginConfig aspectPluginConfig) {
    super(aspectPluginConfig);
  }

  @Override
  protected Stream<UpsertItem> applyMCPSideEffect(
      UpsertItem input, EntityRegistry entityRegistry, @Nonnull AspectRetriever aspectRetriever) {
    // Mirror aspects to another URN in SQL & Search
    Urn mirror = UrnUtils.getUrn(input.getUrn().toString().replace(",PROD)", ",DEV)"));
    return Stream.of(
        MCPUpsertBatchItem.builder()
            .urn(mirror)
            .aspectName(input.getAspectName())
            .aspect(input.getAspect())
            .auditStamp(input.getAuditStamp())
            .systemMetadata(input.getSystemMetadata())
            .build(entityRegistry, aspectRetriever));
  }
}
