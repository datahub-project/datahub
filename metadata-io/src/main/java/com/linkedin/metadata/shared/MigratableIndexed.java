package com.linkedin.metadata.shared;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.UnaryOperator;
import javax.annotation.Nonnull;

/**
 * An index family a cluster migration can build on another cluster from its own mappings: graph,
 * system metadata and timeseries. Entity search indices move between clusters through the Search V3
 * fill instead and do not implement this.
 */
public interface MigratableIndexed extends ElasticSearchIndexed {
  /**
   * The index configurations of {@link #buildReindexConfigs}, built for another cluster: {@code
   * targetBuilder} inspects that cluster's live state and supplies its shard and replica counts,
   * and {@code nameRewriter} maps each index name onto that cluster's prefix. A family migration
   * uses this to create the target indices from the family's own mappings, so the later deploy that
   * flips the serving cluster sees nothing to change.
   */
  List<ReindexConfig> buildReindexConfigsFor(
      @Nonnull OperationContext opContext,
      Collection<Pair<Urn, StructuredPropertyDefinition>> properties,
      @Nonnull ESIndexBuilder targetBuilder,
      @Nonnull UnaryOperator<String> nameRewriter)
      throws IOException;
}
