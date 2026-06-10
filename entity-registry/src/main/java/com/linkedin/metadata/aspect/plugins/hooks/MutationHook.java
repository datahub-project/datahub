package com.linkedin.metadata.aspect.plugins.hooks;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.PluginSpec;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/** Applies changes to the RecordTemplate prior to write */
public abstract class MutationHook extends PluginSpec {

  /**
   * Priority used to order mutation hooks. Higher values run first. The migration chain uses {@link
   * #MIGRATION_PRIORITY} to guarantee it runs before all other hooks.
   */
  public static final int MIGRATION_PRIORITY = Integer.MAX_VALUE;

  /** Override to control execution order. Default {@code 0}; higher runs first. */
  public int getPriority() {
    return 0;
  }

  /**
   * Mutating hook, original objects are potentially modified.
   *
   * @param operationContext
   * @param changeMCPS input upsert items
   * @param retrieverContext aspect & graph retriever
   * @return all items, with a boolean to indicate mutation
   */
  public final Stream<Pair<ChangeMCP, Boolean>> applyWriteMutation(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPS,
      @Nonnull RetrieverContext retrieverContext) {
    return writeMutation(
        operationContext,
        changeMCPS.stream()
            .filter(i -> shouldApply(i.getChangeType(), i.getEntitySpec(), i.getAspectName()))
            .collect(Collectors.toList()),
        retrieverContext);
  }

  // Read mutation
  public final Stream<Pair<ReadItem, Boolean>> applyReadMutation(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ReadItem> items,
      @Nonnull RetrieverContext retrieverContext) {
    return readMutation(
        operationContext,
        items.stream()
            .filter(i -> isEntityAspectSupported(i.getEntitySpec(), i.getAspectName()))
            .collect(Collectors.toList()),
        retrieverContext);
  }

  /**
   * Apply Proposal mutations prior to validation
   *
   * @param operationContext operation context
   * @param mcpItems wrapper for MCP
   * @param retrieverContext retriever context
   * @return stream of mutated Proposal items
   */
  public final Stream<MCPItem> applyProposalMutation(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<MCPItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    return proposalMutation(
        operationContext,
        mcpItems.stream()
            .filter(i -> shouldApply(i.getChangeType(), i.getEntitySpec(), i.getAspectName()))
            .collect(Collectors.toList()),
        retrieverContext);
  }

  protected Stream<Pair<ReadItem, Boolean>> readMutation(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ReadItem> items,
      @Nonnull RetrieverContext retrieverContext) {
    return items.stream().map(i -> Pair.of(i, false));
  }

  protected Stream<Pair<ChangeMCP, Boolean>> writeMutation(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPS,
      @Nonnull RetrieverContext retrieverContext) {
    return changeMCPS.stream().map(i -> Pair.of(i, false));
  }

  protected Stream<MCPItem> proposalMutation(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<MCPItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }
}
