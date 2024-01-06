package com.linkedin.metadata.aspect.plugins.hooks;

import com.linkedin.metadata.aspect.batch.MCLBatchItem;
import com.linkedin.metadata.aspect.plugins.PluginSpec;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import java.util.List;
import java.util.stream.Stream;

/** Given an MCL produce additional MCLs for writing */
public abstract class MCLSideEffect<L extends MCLBatchItem> extends PluginSpec {

  public MCLSideEffect(AspectPluginConfig aspectPluginConfig) {
    super(aspectPluginConfig);
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

  protected abstract Stream<L> applyMCLSideEffect(L input);
}
