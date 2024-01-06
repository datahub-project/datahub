package com.linkedin.metadata.aspect.plugins.hooks;

import com.linkedin.metadata.aspect.batch.MCLBatchItem;
import com.linkedin.metadata.aspect.plugins.PluginSpec;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/** Given an MCL produce additional MCLs for writing */
public abstract class MCLSideEffect extends PluginSpec {

  public MCLSideEffect(AspectPluginConfig aspectPluginConfig) {
    super(aspectPluginConfig);
  }

  /**
   * Given a list of MCLs, output additional MCLs
   *
   * @param input list
   * @return additional upserts
   */
  public final Stream<MCLBatchItem> apply(
      @Nonnull List<MCLBatchItem> input,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull AspectRetriever aspectRetriever) {
    return input.stream()
        .filter(item -> shouldApply(item.getChangeType(), item.getUrn(), item.getAspectSpec()))
        .flatMap(i -> applyMCLSideEffect(i, entityRegistry, aspectRetriever));
  }

  protected abstract Stream<MCLBatchItem> applyMCLSideEffect(
      @Nonnull MCLBatchItem input,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull AspectRetriever aspectRetriever);
}
