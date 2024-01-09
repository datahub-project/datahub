package com.linkedin.metadata.aspect.plugins.hooks;

import com.linkedin.metadata.aspect.batch.UpsertItem;
import com.linkedin.metadata.aspect.plugins.PluginSpec;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/** Given an MCP produce additional MCPs to write */
public abstract class MCPSideEffect extends PluginSpec {

  public MCPSideEffect(AspectPluginConfig aspectPluginConfig) {
    super(aspectPluginConfig);
  }

  /**
   * Given the list of MCP upserts, output additional upserts
   *
   * @param input list
   * @return additional upserts
   */
  public final Stream<UpsertItem> apply(
      List<UpsertItem> input,
      EntityRegistry entityRegistry,
      @Nonnull AspectRetriever aspectRetriever) {
    return input.stream()
        .filter(item -> shouldApply(item.getChangeType(), item.getUrn(), item.getAspectSpec()))
        .flatMap(i -> applyMCPSideEffect(i, entityRegistry, aspectRetriever));
  }

  protected abstract Stream<UpsertItem> applyMCPSideEffect(
      UpsertItem input, EntityRegistry entityRegistry, @Nonnull AspectRetriever aspectRetriever);
}
