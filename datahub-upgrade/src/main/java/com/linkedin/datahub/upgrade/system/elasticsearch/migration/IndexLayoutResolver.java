package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.search.elasticsearch.update.MigrationIndexNames;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Resolves a family's logical index names on a cluster to the indices that hold their documents.
 * Graph, system metadata and timeseries indices are concrete (an alias over one ZDU backing index
 * still copies through the alias name); usage events are a rollover alias on OpenSearch and a data
 * stream on Elasticsearch.
 */
public final class IndexLayoutResolver {
  private IndexLayoutResolver() {}

  /** Null when the logical name does not exist on the cluster at all. */
  @Nullable
  public static SourceIndexSet resolve(
      @Nonnull final ClusterHttp cluster,
      @Nonnull final SearchComponent family,
      @Nonnull final String logicalName)
      throws IOException {
    if (family == SearchComponent.USAGE) {
      // Only Elasticsearch keeps usage events in a data stream. The data stream lookup is an admin
      // API that a user scoped to the deployment's own indices need not hold, so OpenSearch, where
      // usage is a rollover alias, is never asked.
      if (!cluster.isOpenSearch()) {
        final List<String> dataStream = cluster.dataStreamIndices(logicalName);
        if (dataStream != null && !dataStream.isEmpty()) {
          return new SourceIndexSet(
              logicalName,
              IndexLayout.DATA_STREAM,
              dataStream,
              dataStream.get(dataStream.size() - 1));
        }
      }
      final List<ClusterHttp.AliasMember> members = cluster.aliasMembers(logicalName);
      if (!members.isEmpty()) {
        final List<String> backing = new ArrayList<>();
        String writeIndex = null;
        for (ClusterHttp.AliasMember member : members) {
          backing.add(member.index());
          if (member.writeIndex()) {
            writeIndex = member.index();
          }
        }
        backing.sort(String::compareTo);
        if (writeIndex == null) {
          writeIndex = lastOwnGeneration(backing);
        }
        return new SourceIndexSet(logicalName, IndexLayout.ROLLOVER_ALIAS, backing, writeIndex);
      }
      // A legacy concrete usage index predates the rollover layout.
      if (cluster.indexExists(logicalName)) {
        return new SourceIndexSet(
            logicalName, IndexLayout.CONCRETE, List.of(logicalName), logicalName);
      }
      return null;
    }
    if (!cluster.indexExists(logicalName)) {
      return null;
    }
    return new SourceIndexSet(logicalName, IndexLayout.CONCRETE, List.of(logicalName), logicalName);
  }

  /**
   * The write index of an alias that names none: a single-member alias still receives writes, and
   * the newest of the alias's own generations is the one. An adopted staging member sorts after
   * them but never takes writes addressed to the alias.
   */
  @Nonnull
  private static String lastOwnGeneration(@Nonnull final List<String> sortedBacking) {
    for (int i = sortedBacking.size() - 1; i >= 0; i--) {
      if (!MigrationIndexNames.isUsageStagingIndex(sortedBacking.get(i))) {
        return sortedBacking.get(i);
      }
    }
    return sortedBacking.get(sortedBacking.size() - 1);
  }
}
