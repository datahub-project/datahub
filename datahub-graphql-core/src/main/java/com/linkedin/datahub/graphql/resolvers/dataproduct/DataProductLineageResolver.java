package com.linkedin.datahub.graphql.resolvers.dataproduct;

import com.linkedin.datahub.graphql.generated.DataProductLineageResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver for {@code DataProduct.dataProductLineage}.
 *
 * <p>Phase 1A stub: the GraphQL schema is registered so the engine boots and frontend codegen picks
 * up the new types, but invoking the field returns a {@link UnsupportedOperationException}. Phase
 * 1B will replace this with the per-member fan-out + owner-bucketing algorithm documented in {@code
 * drillable-lineage-roadmap.md} (decision D2).
 */
@Slf4j
public class DataProductLineageResolver
    implements DataFetcher<CompletableFuture<DataProductLineageResult>> {

  @Override
  public CompletableFuture<DataProductLineageResult> get(
      final DataFetchingEnvironment environment) {
    final CompletableFuture<DataProductLineageResult> failed = new CompletableFuture<>();
    failed.completeExceptionally(
        new UnsupportedOperationException(
            "DataProduct.dataProductLineage is not yet implemented (Phase 1A stub). Tracked as"
                + " Phase 1B of the drillable lineage roadmap."));
    return failed;
  }
}
