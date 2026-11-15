package com.datahub.authorization;

import com.linkedin.common.urn.Urn;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Reads policies for the policy cache.
 *
 * <p>Each policy must carry its URN: the cache updates policies by URN, and {@link PolicySnapshot}
 * uses the URN to break ties in the policy order.
 */
public interface PolicyLoader {

  /**
   * Reads every policy. The list of policies may lag behind recent writes (it comes from the search
   * index), but the content of each policy is read from the primary store.
   */
  @Nonnull
  List<PolicyFetcher.Policy> loadAll();

  /**
   * Reads the given policies by primary key. A URN that is not in the result has no policy, for
   * example because it was deleted.
   */
  @Nonnull
  Map<Urn, PolicyFetcher.Policy> loadByUrns(@Nonnull Set<Urn> urns);
}
