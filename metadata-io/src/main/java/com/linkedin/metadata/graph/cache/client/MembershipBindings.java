package com.linkedin.metadata.graph.cache.client;

import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/** Resolves {@link MembershipReadSpec} instances from live cache bindings. */
public final class MembershipBindings {

  private MembershipBindings() {}

  @Nonnull
  public static MembershipReadSpec membershipSpec(@Nonnull OperationContext opContext) {
    EntityGraphCache cache = opContext.getEntityGraphCache();
    return resolve(opContext)
        .orElseGet(
            () ->
                MembershipReadSpecs.membership(
                    EntityGraphBinding.builder()
                        .graphId(KnownEntityGraph.MEMBERSHIP.getConfigKey())
                        .source(KnownEntityGraph.MEMBERSHIP.getExpectedBuildSource())
                        .build(),
                    relatedTypeFilterFetchCap(cache, KnownEntityGraph.MEMBERSHIP.getConfigKey())));
  }

  @Nonnull
  public static java.util.Optional<MembershipReadSpec> resolve(
      @Nonnull OperationContext opContext) {
    EntityGraphCache cache = opContext.getEntityGraphCache();
    return cache
        .bindingForKnownGraph(KnownEntityGraph.MEMBERSHIP)
        .flatMap(
            binding ->
                MembershipReadSpecs.forKnownGraph(
                    KnownEntityGraph.MEMBERSHIP,
                    binding,
                    relatedTypeFilterFetchCap(cache, binding.getGraphId())));
  }

  private static int relatedTypeFilterFetchCap(
      @Nonnull EntityGraphCache cache, @Nonnull String graphId) {
    return cache
        .maxEdgesForGraph(graphId)
        .orElse(MembershipReadSpec.DEFAULT_RELATED_TYPE_FILTER_FETCH_CAP);
  }
}
