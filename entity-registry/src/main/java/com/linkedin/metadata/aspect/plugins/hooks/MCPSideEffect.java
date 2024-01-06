package com.linkedin.metadata.aspect.plugins.hooks;

import com.linkedin.metadata.aspect.batch.SystemAspect;
import com.linkedin.metadata.aspect.batch.UpsertItem;
import com.linkedin.metadata.aspect.plugins.PluginSpec;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import java.util.List;
import java.util.stream.Stream;

/** Given an MCP produce additional MCPs to write */
public abstract class MCPSideEffect<U extends UpsertItem<S>, S extends SystemAspect>
    extends PluginSpec {

  public MCPSideEffect(AspectPluginConfig aspectPluginConfig) {
    super(aspectPluginConfig);
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
