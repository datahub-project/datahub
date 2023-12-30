package com.linkedin.metadata.aspect.plugins.hooks;

import com.linkedin.metadata.aspect.batch.SystemAspect;
import com.linkedin.metadata.aspect.batch.UpsertItem;
import com.linkedin.metadata.aspect.plugins.ConfigurableEntityAspectPlugin;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import java.util.List;
import java.util.stream.Stream;
import lombok.Getter;

/** Given an MCP produce additional MCPs to write */
@Getter
public abstract class MCPSideEffect<U extends UpsertItem<S>, S extends SystemAspect>
    implements ConfigurableEntityAspectPlugin {
  private final AspectPluginConfig config;

  protected MCPSideEffect(AspectPluginConfig config) {
    this.config = config;
  }

  /**
   * Given the list of MCP upserts, output additional upserts
   *
   * @param input list
   * @return additional upserts
   */
  public final Stream<U> apply(List<U> input) {
    return input.stream()
        .filter(item -> shouldApply(item.getChangeType(), item.getUrn(), item.getAspectSpec()))
        .flatMap(this::applyMCPSideEffect);
  }

  protected abstract Stream<U> applyMCPSideEffect(U input);
}
