package com.linkedin.metadata.aspect.plugins.hooks;

import com.linkedin.metadata.aspect.batch.MCLBatchItem;
import com.linkedin.metadata.aspect.plugins.ConfigurableEntityAspectPlugin;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import java.util.List;
import java.util.stream.Stream;
import lombok.Getter;

/** Given an MCL produce additional MCLs for writing */
@Getter
public abstract class MCLSideEffect<L extends MCLBatchItem>
    implements ConfigurableEntityAspectPlugin {
  private final AspectPluginConfig config;

  protected MCLSideEffect(AspectPluginConfig config) {
    this.config = config;
  }

  /**
   * Given a list of MCLs, output additional MCLs
   *
   * @param input list
   * @return additional upserts
   */
  public final Stream<L> apply(List<L> input) {
    return input.stream()
        .filter(item -> shouldApply(item.getChangeType(), item.getUrn(), item.getAspectSpec()))
        .flatMap(this::applyMCLSideEffect);
  }

  abstract Stream<L> applyMCLSideEffect(L input);
}
