package com.linkedin.metadata.aspect.plugins.hooks;

import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.PluginSpec;
import java.util.Collection;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/** Given an MCP produce additional MCPs to write */
public abstract class MCPSideEffect extends PluginSpec
    implements BiFunction<Collection<ChangeMCP>, RetrieverContext, Stream<ChangeMCP>> {

  /**
   * Given the list of MCP upserts, output additional upserts
   *
   * @param changeMCPS list
   * @return additional upserts
   */
  public final Stream<ChangeMCP> apply(
      Collection<ChangeMCP> changeMCPS, @Nonnull RetrieverContext retrieverContext) {
    return applyMCPSideEffect(
        changeMCPS.stream()
            .filter(item -> shouldApply(item.getChangeType(), item.getUrn(), item.getAspectSpec()))
            .collect(Collectors.toList()),
        retrieverContext);
  }

  protected abstract Stream<ChangeMCP> applyMCPSideEffect(
      Collection<ChangeMCP> changeMCPS, @Nonnull RetrieverContext retrieverContext);
}
