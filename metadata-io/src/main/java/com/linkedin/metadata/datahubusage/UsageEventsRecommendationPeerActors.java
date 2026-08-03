package com.linkedin.metadata.datahubusage;

import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;

/** Shared actor / peer scoping for usage-based recommendation queries (OpenSearch and SQL). */
public final class UsageEventsRecommendationPeerActors {

  private UsageEventsRecommendationPeerActors() {}

  /** Returns peer corp user URNs as strings when peer-group restrictions apply; otherwise null. */
  @Nullable
  public static List<String> peerActorUrns(@Nonnull OperationContext opContext) {
    ViewAuthorizationConfiguration config =
        opContext.getOperationContextConfig().getViewAuthorizationConfiguration();

    if (config.isEnabled()
        && config.getRecommendations().isPeerGroupEnabled()
        && !opContext.isSystemAuth()) {
      return opContext.getActorPeers().stream().map(Object::toString).collect(Collectors.toList());
    }
    return null;
  }

  /**
   * Restricts usage events to a set of actors when peer-group visibility is enabled; empty if no
   * restriction.
   */
  public static Optional<QueryBuilder> restrictPeersTermsQuery(
      @Nonnull OperationContext opContext) {
    List<String> peers = peerActorUrns(opContext);
    if (peers != null && !peers.isEmpty()) {
      return Optional.of(
          QueryBuilders.termsQuery(DataHubUsageEventConstants.ACTOR_URN + ".keyword", peers));
    }
    return Optional.empty();
  }
}
