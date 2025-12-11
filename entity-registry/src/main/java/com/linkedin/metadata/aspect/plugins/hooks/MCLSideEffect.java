/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.aspect.plugins.hooks;

import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.plugins.PluginSpec;
import java.util.Collection;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/** Given an MCL produce additional MCLs for writing */
public abstract class MCLSideEffect extends PluginSpec
    implements BiFunction<Collection<MCLItem>, RetrieverContext, Stream<MCLItem>> {

  /**
   * Given a list of MCLs, output additional MCLs
   *
   * @param batchItems list
   * @return additional upserts
   */
  @Override
  public final Stream<MCLItem> apply(
      @Nonnull Collection<MCLItem> batchItems, @Nonnull RetrieverContext retrieverContext) {
    return applyMCLSideEffect(
        batchItems.stream()
            .filter(item -> shouldApply(item.getChangeType(), item.getUrn(), item.getAspectName()))
            .collect(Collectors.toList()),
        retrieverContext);
  }

  protected abstract Stream<MCLItem> applyMCLSideEffect(
      @Nonnull Collection<MCLItem> batchItems, @Nonnull RetrieverContext retrieverContext);
}
