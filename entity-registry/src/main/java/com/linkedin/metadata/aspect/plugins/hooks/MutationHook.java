package com.linkedin.metadata.aspect.plugins.hooks;

import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.PluginSpec;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/** Applies changes to the RecordTemplate prior to write */
public abstract class MutationHook extends PluginSpec {

  public MutationHook(AspectPluginConfig aspectPluginConfig) {
    super(aspectPluginConfig);
  }

  /**
   * Mutating hook, original objects are potentially modified.
   *
   * @param changeMCPS input upsert items
   * @param aspectRetriever aspect retriever
   * @return all items, with a boolean to indicate mutation
   */
  public final Stream<Pair<ChangeMCP, Boolean>> applyWriteMutation(
      @Nonnull Collection<ChangeMCP> changeMCPS, @Nonnull AspectRetriever aspectRetriever) {
    return writeMutation(
        changeMCPS.stream()
            .filter(i -> shouldApply(i.getChangeType(), i.getEntitySpec(), i.getAspectSpec()))
            .collect(Collectors.toList()),
        aspectRetriever);
  }

  // Read mutation
  public final Stream<Pair<ReadItem, Boolean>> applyReadMutation(
      @Nonnull Collection<ReadItem> items, @Nonnull AspectRetriever aspectRetriever) {
    return readMutation(
        items.stream()
            .filter(i -> isEntityAspectSupported(i.getEntitySpec(), i.getAspectSpec()))
            .collect(Collectors.toList()),
        aspectRetriever);
  }

  protected Stream<Pair<ReadItem, Boolean>> readMutation(
      @Nonnull Collection<ReadItem> items, @Nonnull AspectRetriever aspectRetriever) {
    return items.stream().map(i -> Pair.of(i, false));
  }

  protected Stream<Pair<ChangeMCP, Boolean>> writeMutation(
      @Nonnull Collection<ChangeMCP> changeMCPS, @Nonnull AspectRetriever aspectRetriever) {
    return changeMCPS.stream().map(i -> Pair.of(i, false));
  }
}
