package com.linkedin.datahub.graphql.resolvers.lineage.aggregate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.datahub.authorization.AuthorizationConfiguration;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Restricted;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.search.LineageSearchEntity;
import com.linkedin.metadata.search.LineageSearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.RestrictedService;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Shared algorithm for resolvers that aggregate lineage by walking each member of a source Domain
 * or DataProduct, running per-member {@code searchAcrossLineage}, and bucketing the resulting
 * neighbour hits by their owning Domain or DataProduct.
 *
 * <p>Subclasses bind the auto-generated GraphQL input type, enumerate the source's members, and
 * marshal the canonical {@link AggregatedLineageResponse} into the auto-generated GraphQL result
 * type. Everything else — owner resolution, auth filtering, restricted-wrapping, sorting,
 * pagination, partial-result tracking — lives here.
 *
 * <p>The algorithm intentionally matches decision D2 in {@code drillable-lineage-roadmap.md}. See
 * that document for the rationale behind the order of operations (sort happens upfront over all
 * buckets, not per-page; pagination is applied last so ordering is stable across "Show More"
 * requests).
 *
 * @param <I> the auto-generated GraphQL input type (e.g. {@code DomainLineageInput})
 * @param <R> the auto-generated GraphQL result type (e.g. {@code DomainLineageResult})
 */
@Slf4j
public abstract class AggregatedLineageResolver<I, R> implements DataFetcher<CompletableFuture<R>> {

  protected static final String INPUT_ARG_NAME = "input";

  /** Server-side clamp ceilings. The defaults live in the GraphQL input definitions. */
  static final int MAX_MEMBER_SCAN_CAP = 5_000;

  static final int MAX_PER_MEMBER_COUNT = 1_000;
  static final int MAX_COUNT = 200;
  static final int MAX_HOPS = 20;

  protected final EntityClient entityClient;
  protected final RestrictedService restrictedService;
  protected final AuthorizationConfiguration authorizationConfiguration;

  protected AggregatedLineageResolver(
      final EntityClient entityClient,
      final RestrictedService restrictedService,
      final AuthorizationConfiguration authorizationConfiguration) {
    this.entityClient = entityClient;
    this.restrictedService = restrictedService;
    this.authorizationConfiguration = authorizationConfiguration;
  }

  // ---------------------------------------------------------------------------
  // Subclass extension points
  // ---------------------------------------------------------------------------

  /** The auto-generated input class to bind against {@code environment.getArgument("input")}. */
  protected abstract Class<I> getInputClass();

  /** The source URN extracted from {@code environment.getSource()} (the parent type). */
  protected abstract Urn extractSourceUrn(DataFetchingEnvironment environment);

  /** Convert the auto-generated input into the canonical, pre-clamped request shape. */
  protected abstract AggregatedLineageRequest toCanonicalRequest(I input, Urn sourceUrn);

  /**
   * Subclass-specific member enumeration (DataProductProperties.assets or searchAcrossEntities).
   */
  protected abstract MembersResult enumerateMembers(
      QueryContext context, Urn sourceUrn, int memberScanCap);

  /**
   * Subclass picks the right strategy. Domain may pick either based on {@code
   * input.groupByDataProduct}; DataProduct always picks the DataProduct strategy.
   */
  protected abstract OwnerResolutionStrategy resolveOwnerStrategy(
      I input, AggregatedLineageRequest request);

  /** Build the auto-generated GraphQL result type from the canonical response. */
  protected abstract R buildResult(AggregatedLineageResponse response);

  /** Per-member search entity-type filter (typically the lineage-bearing subset). */
  protected abstract List<String> getNeighbourEntityTypes();

  // ---------------------------------------------------------------------------
  // Algorithm
  // ---------------------------------------------------------------------------

  @Override
  public final CompletableFuture<R> get(final DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final Urn sourceUrn = extractSourceUrn(environment);
    final I input = bindArgument(environment.getArgument(INPUT_ARG_NAME), getInputClass());
    final AggregatedLineageRequest request = clamp(toCanonicalRequest(input, sourceUrn));
    final OwnerResolutionStrategy ownerStrategy = resolveOwnerStrategy(input, request);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> aggregate(context, request, ownerStrategy), this.getClass().getSimpleName(), "get");
  }

  /** Visible for testing. */
  AggregatedLineageRequest clamp(final AggregatedLineageRequest req) {
    return req.toBuilder()
        .hops(Math.max(1, Math.min(req.getHops(), MAX_HOPS)))
        .start(Math.max(0, req.getStart()))
        .count(Math.max(1, Math.min(req.getCount(), MAX_COUNT)))
        .perMemberCount(Math.max(1, Math.min(req.getPerMemberCount(), MAX_PER_MEMBER_COUNT)))
        .memberScanCap(Math.max(1, Math.min(req.getMemberScanCap(), MAX_MEMBER_SCAN_CAP)))
        .build();
  }

  private R aggregate(
      final QueryContext context,
      final AggregatedLineageRequest request,
      final OwnerResolutionStrategy ownerStrategy) {

    // 1. Enumerate members (subclass-specific).
    final MembersResult members =
        enumerateMembers(context, request.getSourceUrn(), request.getMemberScanCap());
    boolean isPartial = members.getTotal() > members.getUrns().size();

    if (members.getUrns().isEmpty()) {
      return buildResult(emptyResponse(request, members, isPartial));
    }

    // 2. Per-member fan-out (parallel).
    final List<LineageSearchResult> perMemberResults =
        fanOutSearchAcrossLineage(context, request, members.getUrns());
    for (final LineageSearchResult r : perMemberResults) {
      if (r != null && Boolean.TRUE.equals(r.isIsPartial())) {
        isPartial = true;
      }
    }

    // 3. Collect distinct neighbour URNs (across all members) and resolve owners in one shot.
    final Map<Urn, LineageHit> hitsByNeighbour =
        collectHits(request, members.getUrns(), perMemberResults);
    if (hitsByNeighbour.isEmpty()) {
      return buildResult(emptyResponse(request, members, isPartial));
    }
    final Map<Urn, Set<Urn>> ownersByNeighbour =
        ownerStrategy.resolveOwners(context.getOperationContext(), hitsByNeighbour.keySet());

    // 4. Bucket hits by owner; L2: drop hits with no resolvable owner and set isPartial.
    final Map<Urn, OwnerBucket> bucketsByOwner = new HashMap<>();
    for (final Map.Entry<Urn, LineageHit> entry : hitsByNeighbour.entrySet()) {
      final Urn neighbourUrn = entry.getKey();
      final LineageHit hit = entry.getValue();
      final Set<Urn> owners = ownersByNeighbour.get(neighbourUrn);
      if (owners == null || owners.isEmpty()) {
        isPartial = true; // L2 — hit dropped because no owner could be resolved.
        continue;
      }
      for (final Urn ownerUrn : owners) {
        if (ownerUrn.equals(request.getSourceUrn())) {
          continue; // L1 — self-loop suppression.
        }
        final OwnerBucket bucket = bucketsByOwner.computeIfAbsent(ownerUrn, OwnerBucket::new);
        bucket.memberMatches.addAll(hit.contributingMembers);
        bucket.neighbourEntities.add(neighbourUrn);
        bucket.degreeMin = Math.min(bucket.degreeMin, hit.minDegree);
        bucket.degreeMax = Math.max(bucket.degreeMax, hit.maxDegree);
      }
    }

    // 5. Authorisation: swap-or-drop unauthorized owners.
    final Map<Urn, OwnerBucket> finalBuckets =
        applyAuthorization(context.getOperationContext(), request, bucketsByOwner);

    // 6. Sort all buckets, then paginate the user-requested page.
    final List<OwnerBucket> sorted =
        finalBuckets.values().stream()
            .sorted(
                Comparator.comparingInt((OwnerBucket b) -> b.memberMatches.size())
                    .reversed()
                    .thenComparing(b -> b.ownerUrn.toString()))
            .collect(Collectors.toList());

    final int total = sorted.size();
    final int from = Math.min(request.getStart(), total);
    final int to = Math.min(from + request.getCount(), total);
    final List<OwnerBucket> page = sorted.subList(from, to);

    // 7. Map to canonical Relationship rows.
    final List<AggregatedLineageResponse.Relationship> relationships =
        page.stream().map(b -> toRelationship(context, request, b)).collect(Collectors.toList());

    return buildResult(
        AggregatedLineageResponse.builder()
            .start(request.getStart())
            .count(relationships.size())
            .total(total)
            .memberScanCount(members.getUrns().size())
            .memberTotal(members.getTotal())
            .isPartial(isPartial)
            .direction(request.getDirection())
            .relationships(relationships)
            .build());
  }

  // ---------------------------------------------------------------------------
  // Fan-out / hit collection helpers
  // ---------------------------------------------------------------------------

  private List<LineageSearchResult> fanOutSearchAcrossLineage(
      final QueryContext context, final AggregatedLineageRequest request, final List<Urn> members) {
    final OperationContext opContext = applyFlags(context.getOperationContext(), request);
    final com.linkedin.metadata.query.filter.Filter filter =
        ResolverUtils.buildFilter(null, request.getOrFilters());
    final com.linkedin.metadata.graph.LineageDirection internalDirection =
        com.linkedin.metadata.graph.LineageDirection.valueOf(request.getDirection().name());
    final List<String> entityNames = getNeighbourEntityTypes();
    final ExecutorService executor = GraphQLConcurrencyUtils.getExecutorService();

    final List<CompletableFuture<LineageSearchResult>> futures =
        members.stream()
            .map(
                memberUrn -> {
                  final java.util.function.Supplier<LineageSearchResult> task =
                      () -> {
                        try {
                          return entityClient.searchAcrossLineage(
                              opContext,
                              memberUrn,
                              internalDirection,
                              entityNames,
                              "*",
                              request.getHops(),
                              filter,
                              Collections.emptyList(),
                              0,
                              request.getPerMemberCount());
                        } catch (Exception e) {
                          log.warn(
                              "searchAcrossLineage failed for member {} (direction={}, hops={}); skipping.",
                              memberUrn,
                              request.getDirection(),
                              request.getHops(),
                              e);
                          return null;
                        }
                      };
                  return executor != null
                      ? CompletableFuture.supplyAsync(task, executor)
                      : CompletableFuture.supplyAsync(task);
                })
            .collect(Collectors.toList());

    return futures.stream().map(CompletableFuture::join).collect(Collectors.toList());
  }

  private static OperationContext applyFlags(
      final OperationContext base, final AggregatedLineageRequest request) {
    OperationContext ctx = base;
    if (request.getSearchFlags() != null) {
      final com.linkedin.metadata.query.SearchFlags wireFlags =
          mapSearchFlags(request.getSearchFlags());
      ctx = ctx.withSearchFlags(flags -> wireFlags);
    }
    if (request.getLineageFlags() != null) {
      final com.linkedin.metadata.query.LineageFlags wireFlags =
          mapLineageFlags(request.getLineageFlags());
      ctx = ctx.withLineageFlags(flags -> wireFlags);
    }
    return ctx;
  }

  /**
   * Translate the auto-generated GraphQL {@link com.linkedin.datahub.graphql.generated.SearchFlags}
   * into the PDL {@link com.linkedin.metadata.query.SearchFlags}. Only the subset relevant to
   * lineage walks is forwarded; the rest is left at server defaults so the caller can't bypass
   * lineage-specific guards.
   */
  private static com.linkedin.metadata.query.SearchFlags mapSearchFlags(
      final com.linkedin.datahub.graphql.generated.SearchFlags graphqlFlags) {
    final com.linkedin.metadata.query.SearchFlags wire =
        new com.linkedin.metadata.query.SearchFlags();
    if (graphqlFlags.getSkipCache() != null) {
      wire.setSkipCache(graphqlFlags.getSkipCache());
    }
    if (graphqlFlags.getSkipAggregates() != null) {
      wire.setSkipAggregates(graphqlFlags.getSkipAggregates());
    }
    if (graphqlFlags.getSkipHighlighting() != null) {
      wire.setSkipHighlighting(graphqlFlags.getSkipHighlighting());
    }
    return wire;
  }

  private static com.linkedin.metadata.query.LineageFlags mapLineageFlags(
      final com.linkedin.datahub.graphql.generated.LineageFlags graphqlFlags) {
    final com.linkedin.metadata.query.LineageFlags wire =
        new com.linkedin.metadata.query.LineageFlags();
    if (graphqlFlags.getStartTimeMillis() != null) {
      wire.setStartTimeMillis(graphqlFlags.getStartTimeMillis().longValue());
    }
    if (graphqlFlags.getEndTimeMillis() != null) {
      wire.setEndTimeMillis(graphqlFlags.getEndTimeMillis().longValue());
    }
    if (graphqlFlags.getEntitiesExploredPerHopLimit() != null) {
      wire.setEntitiesExploredPerHopLimit(graphqlFlags.getEntitiesExploredPerHopLimit());
    }
    return wire;
  }

  private static Map<Urn, LineageHit> collectHits(
      final AggregatedLineageRequest request,
      final List<Urn> members,
      final List<LineageSearchResult> perMemberResults) {
    final Map<Urn, LineageHit> byNeighbour = new HashMap<>();
    for (int i = 0; i < members.size(); i++) {
      final Urn memberUrn = members.get(i);
      final LineageSearchResult result = perMemberResults.get(i);
      if (result == null || result.getEntities() == null) {
        continue;
      }
      for (final LineageSearchEntity entity : result.getEntities()) {
        final Urn neighbourUrn = entity.getEntity();
        if (neighbourUrn == null) {
          continue;
        }
        if (members.contains(neighbourUrn)) {
          continue; // intra-source lineage; not a "neighbour" relationship.
        }
        final int degree =
            entity.getDegrees() != null && !entity.getDegrees().isEmpty()
                ? entity.getDegrees().stream().mapToInt(Integer::intValue).min().orElse(1)
                : 1;
        final LineageHit hit = byNeighbour.computeIfAbsent(neighbourUrn, LineageHit::new);
        hit.contributingMembers.add(memberUrn);
        hit.minDegree = Math.min(hit.minDegree, degree);
        hit.maxDegree = Math.max(hit.maxDegree, degree);
      }
    }
    return byNeighbour;
  }

  // ---------------------------------------------------------------------------
  // Authorisation
  // ---------------------------------------------------------------------------

  private Map<Urn, OwnerBucket> applyAuthorization(
      final OperationContext opContext,
      final AggregatedLineageRequest request,
      final Map<Urn, OwnerBucket> buckets) {
    if (!authorizationConfiguration.getView().isEnabled() || opContext.isSystemAuth()) {
      return buckets;
    }
    final Map<Urn, OwnerBucket> out = new HashMap<>(buckets.size());
    for (final Map.Entry<Urn, OwnerBucket> entry : buckets.entrySet()) {
      final Urn ownerUrn = entry.getKey();
      if (AuthorizationUtils.canView(opContext, ownerUrn)) {
        out.put(ownerUrn, entry.getValue());
      } else if (request.isIncludeRestricted()) {
        final Urn encrypted = restrictedService.encryptRestrictedUrn(ownerUrn);
        out.put(encrypted, entry.getValue().rekey(encrypted));
      }
      // else: dropped
    }
    return out;
  }

  // ---------------------------------------------------------------------------
  // Relationship marshalling
  // ---------------------------------------------------------------------------

  private AggregatedLineageResponse.Relationship toRelationship(
      final QueryContext context,
      final AggregatedLineageRequest request,
      final OwnerBucket bucket) {
    final Entity entity = buildEntity(context, bucket.ownerUrn);
    return AggregatedLineageResponse.Relationship.builder()
        .entity(entity)
        .memberMatchCount(bucket.memberMatches.size())
        .neighbourEntityCount(bucket.neighbourEntities.size())
        .degreeMin(bucket.degreeMin)
        .degreeMax(bucket.degreeMax)
        .build();
  }

  private Entity buildEntity(final QueryContext context, final Urn ownerUrn) {
    if (RestrictedService.RESTRICTED_ENTITY_TYPE.equals(ownerUrn.getEntityType())) {
      final Restricted restricted = new Restricted();
      restricted.setType(EntityType.RESTRICTED);
      restricted.setUrn(ownerUrn.toString());
      return restricted;
    }
    final Entity mapped = UrnToEntityMapper.map(context, ownerUrn);
    if (mapped != null) {
      return mapped;
    }
    // Fallback: a minimal Entity-shaped placeholder so the schema's non-null constraint holds even
    // if the URN couldn't be mapped (unknown entity type). Set type=RESTRICTED so the frontend
    // treats it conservatively.
    final Restricted fallback = new Restricted();
    fallback.setType(EntityType.RESTRICTED);
    fallback.setUrn(ownerUrn.toString());
    return fallback;
  }

  private AggregatedLineageResponse emptyResponse(
      final AggregatedLineageRequest request,
      final MembersResult members,
      final boolean isPartial) {
    return AggregatedLineageResponse.builder()
        .start(request.getStart())
        .count(0)
        .total(0)
        .memberScanCount(members.getUrns().size())
        .memberTotal(members.getTotal())
        .isPartial(isPartial || members.getTotal() > members.getUrns().size())
        .direction(request.getDirection())
        .relationships(Collections.emptyList())
        .build();
  }

  // ---------------------------------------------------------------------------
  // Small protected helpers used by subclasses for member enumeration.
  // ---------------------------------------------------------------------------

  protected static String resolveEntityName(final EntityType entityType) {
    return EntityTypeMapper.getName(entityType);
  }

  // ---------------------------------------------------------------------------
  // Internal POJOs
  // ---------------------------------------------------------------------------

  private static final class LineageHit {
    @SuppressWarnings("unused")
    final Urn neighbourUrn;

    final Set<Urn> contributingMembers = new HashSet<>();
    int minDegree = Integer.MAX_VALUE;
    int maxDegree = Integer.MIN_VALUE;

    LineageHit(final Urn neighbourUrn) {
      this.neighbourUrn = neighbourUrn;
    }
  }

  private static final class OwnerBucket {
    Urn ownerUrn;
    final Set<Urn> memberMatches = new HashSet<>();
    final Set<Urn> neighbourEntities = new HashSet<>();
    int degreeMin = Integer.MAX_VALUE;
    int degreeMax = Integer.MIN_VALUE;

    OwnerBucket(final Urn ownerUrn) {
      this.ownerUrn = ownerUrn;
    }

    /**
     * Used when an owner gets restricted-wrapped: we keep the bucket contents but swap the key URN
     * so downstream rendering picks up the encrypted URN.
     */
    OwnerBucket rekey(final Urn newOwnerUrn) {
      final OwnerBucket rekeyed = new OwnerBucket(newOwnerUrn);
      rekeyed.memberMatches.addAll(this.memberMatches);
      rekeyed.neighbourEntities.addAll(this.neighbourEntities);
      rekeyed.degreeMin = this.degreeMin;
      rekeyed.degreeMax = this.degreeMax;
      return rekeyed;
    }
  }

  /** Convenience for subclasses that want to short-circuit on null/empty. */
  @SuppressWarnings("unused")
  @Nullable
  protected static <T> Collection<T> nullOrEmpty(final Collection<T> collection) {
    return collection == null || collection.isEmpty() ? null : collection;
  }
}
