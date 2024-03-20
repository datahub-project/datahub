package com.linkedin.metadata.aspect.plugins.hooks;

import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.plugins.PluginSpec;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import java.util.Collection;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/** Given an MCL produce additional MCLs for writing */
public abstract class MCLSideEffect extends PluginSpec
    implements BiFunction<Collection<MCLItem>, AspectRetriever, Stream<MCLItem>> {

  public MCLSideEffect(AspectPluginConfig aspectPluginConfig) {
    super(aspectPluginConfig);
  }

  /**
   * Given a list of MCLs, output additional MCLs
   *
   * @param batchItems list
   * @return additional upserts
   */
  @Override
  public final Stream<MCLItem> apply(
      @Nonnull Collection<MCLItem> batchItems, @Nonnull AspectRetriever aspectRetriever) {
    return applyMCLSideEffect(
        batchItems.stream()
            .filter(item -> shouldApply(item.getChangeType(), item.getUrn(), item.getAspectSpec()))
            .collect(Collectors.toList()),
        aspectRetriever);
  }

  protected abstract Stream<MCLItem> applyMCLSideEffect(
      @Nonnull Collection<MCLItem> batchItems, @Nonnull AspectRetriever aspectRetriever);
}
