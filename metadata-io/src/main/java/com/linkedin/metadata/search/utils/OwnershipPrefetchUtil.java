package com.linkedin.metadata.search.utils;

import com.linkedin.common.Owner;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.Constants;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Request-scoped ownership resolution primitives backed by the per-request cache on {@link
 * io.datahubproject.metadata.context.AuthorizationContext}. A resource's ownership is
 * actor-independent, so it is fetched at most once per request even when a page of results
 * authorizes the same resource for many privileges or ownership policies.
 *
 * <p>Reads/writes go through {@code opContext.getAuthorizationContext().getResourceOwnersByUrn()},
 * so the cache is shared across {@link ESAccessControlUtil} (which prefetches in bulk), {@code
 * PolicyEngine} (which reads it during owner matching), and {@code OwnerFieldResolverProvider}
 * within the same request.
 */
@Slf4j
public class OwnershipPrefetchUtil {
  private OwnershipPrefetchUtil() {}

  /**
   * Returns the cached owners for {@code urn}, fetching once (single aspect lookup) on a miss.
   * Negative results are cached as an empty list to prevent re-fetching. Uses the request-scoped
   * {@link com.linkedin.metadata.aspect.AspectRetriever} on {@code opContext}.
   */
  @Nonnull
  public static List<Owner> getOwners(@Nonnull OperationContext opContext, @Nonnull Urn urn) {
    final ConcurrentHashMap<Urn, List<Owner>> cache =
        opContext.getAuthorizationContext().getResourceOwnersByUrn();
    return cache.computeIfAbsent(urn, key -> fetchOwners(opContext, key));
  }

  /**
   * Bulk-prefetches ownership for {@code urns}, grouping by entity type and issuing ONE batch
   * aspect lookup per type, seeding the request-scoped cache. URNs already cached are skipped. A
   * failure fetching one entity type does not abort the rest. Negative results (no ownership) are
   * seeded as empty lists so the per-result path does not re-fetch them.
   */
  public static void prefetchOwners(
      @Nonnull OperationContext opContext, @Nonnull Collection<Urn> urns) {
    final ConcurrentHashMap<Urn, List<Owner>> cache =
        opContext.getAuthorizationContext().getResourceOwnersByUrn();

    final Set<Urn> uncached =
        urns.stream().filter(urn -> !cache.containsKey(urn)).collect(Collectors.toSet());
    if (uncached.isEmpty()) {
      return;
    }

    final Map<String, Set<Urn>> urnsByType =
        uncached.stream().collect(Collectors.groupingBy(Urn::getEntityType, Collectors.toSet()));

    for (Map.Entry<String, Set<Urn>> entry : urnsByType.entrySet()) {
      final Set<Urn> typeUrns = entry.getValue();
      try {
        final Map<Urn, Map<String, Aspect>> fetched =
            opContext
                .getAspectRetriever()
                .getLatestAspectObjects(
                    opContext, typeUrns, Collections.singleton(Constants.OWNERSHIP_ASPECT_NAME));
        // Seed every requested urn: present ones with their owners, missing ones with an empty
        // list (negative cache). Use putIfAbsent so a concurrent lazy fetch is not clobbered.
        for (Urn urn : typeUrns) {
          final List<Owner> owners = toOwners(fetched.get(urn));
          cache.putIfAbsent(urn, owners);
        }
      } catch (Exception e) {
        // One bad entity type must not abort prefetch for the rest; the per-result path will then
        // resolve these lazily via getOwners(...).
        log.warn(
            "ownership prefetch failed for entity type {} ({} urns); continuing",
            entry.getKey(),
            typeUrns.size(),
            e);
      }
    }
  }

  @Nonnull
  private static List<Owner> fetchOwners(@Nonnull OperationContext opContext, @Nonnull Urn urn) {
    try {
      final Aspect ownershipAspect =
          opContext
              .getAspectRetriever()
              .getLatestAspectObject(opContext, urn, Constants.OWNERSHIP_ASPECT_NAME);
      return toOwners(
          ownershipAspect == null
              ? null
              : Collections.singletonMap(Constants.OWNERSHIP_ASPECT_NAME, ownershipAspect));
    } catch (Exception e) {
      log.warn("Error while retrieving ownership aspect for urn {}", urn, e);
      return Collections.emptyList();
    }
  }

  @Nonnull
  private static List<Owner> toOwners(Map<String, Aspect> aspectsByName) {
    if (aspectsByName == null) {
      return Collections.emptyList();
    }
    final Aspect ownershipAspect = aspectsByName.get(Constants.OWNERSHIP_ASPECT_NAME);
    if (ownershipAspect == null) {
      return Collections.emptyList();
    }
    // Defensive copy so the cached list is independent of the fetched DataMap-backed list.
    return new java.util.ArrayList<>(new Ownership(ownershipAspect.data()).getOwners());
  }

  /**
   * Unused helper kept private; exposed set of owner-urn strings for field-resolver style callers.
   */
  @Nonnull
  public static Set<String> getOwnerUrns(@Nonnull OperationContext opContext, @Nonnull Urn urn) {
    return getOwners(opContext, urn).stream()
        .map(owner -> owner.getOwner().toString())
        .collect(Collectors.toCollection(HashSet::new));
  }
}
