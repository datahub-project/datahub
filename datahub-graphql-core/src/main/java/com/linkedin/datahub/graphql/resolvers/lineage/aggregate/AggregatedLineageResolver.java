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
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.search.LineageSearchEntity;
import com.linkedin.metadata.search.LineageSearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.RestrictedService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Shared algorithm for resolvers that aggregate lineage by walking each member of a source Domain
 * or DataProduct and bucketing the resulting neighbours by their owning Domain or DataProduct.
 * Subclasses bind input/output types, enumerate the source's members, and pick an {@link
 * OwnerResolutionStrategy}; everything else lives here.
 *
 * @param <I> auto-generated GraphQL input type (e.g. {@code DomainLineageInput})
 * @param <R> auto-generated GraphQL result type (e.g. {@code DomainLineageResult})
 */
@Slf4j
public abstract class AggregatedLineageResolver<I, R> implements DataFetcher<CompletableFuture<R>> {

  protected static final String INPUT_ARG_NAME = "input";

  static final int MAX_MEMBER_SCAN_CAP = 5_000;
  static final int MAX_PER_MEMBER_COUNT = 1_000;
  static final int MAX_COUNT = 200;
  static final int MAX_HOPS = 20;

  /**
   * Maximum concurrent {@code searchAcrossLineage} calls in flight. Bounds load on the shared
   * GraphQL executor and on Elasticsearch when a Domain has thousands of members.
   */
  static final int MAX_PARALLEL_FANOUT = 16;

  /**
   * Maximum distinct neighbour URNs we resolve owners for. Above this we keep the neighbours with
   * the most contributing members (they dominate the sort anyway) and set {@code isPartial=true}.
   * Bounds the worst case for {@link DataProductOwnerResolutionStrategy}, which is O(N) graph
   * calls.
   */
  static final int MAX_OWNER_LOOKUP_NEIGHBOURS = 2_000;

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

  protected abstract Class<I> getInputClass();

  protected abstract Urn extractSourceUrn(DataFetchingEnvironment environment);

  protected abstract AggregatedLineageRequest toCanonicalRequest(I input, Urn sourceUrn);

  protected abstract MembersResult enumerateMembers(
      QueryContext context, Urn sourceUrn, int memberScanCap);

  protected abstract OwnerResolutionStrategy resolveOwnerStrategy(
      I input, AggregatedLineageRequest request);

  protected abstract R buildResult(AggregatedLineageResponse response);

  protected abstract List<String> getNeighbourEntityTypes();

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

    final MembersResult members =
        enumerateMembers(context, request.getSourceUrn(), request.getMemberScanCap());
    boolean isPartial = members.getTotal() > members.getUrns().size();

    if (members.getUrns().isEmpty()) {
      return buildResult(emptyResponse(request, members, isPartial));
    }

    final List<LineageSearchResult> perMemberResults =
        fanOutSearchAcrossLineage(context, request, members.getUrns());
    for (final LineageSearchResult r : perMemberResults) {
      if (r != null && Boolean.TRUE.equals(r.isIsPartial())) {
        isPartial = true;
      }
    }

    final Map<Urn, LineageHit> rawHits = collectHits(members.getUrns(), perMemberResults);
    if (rawHits.isEmpty()) {
      return buildResult(emptyResponse(request, members, isPartial));
    }
    final Map<Urn, LineageHit> hitsByNeighbour;
    if (rawHits.size() > MAX_OWNER_LOOKUP_NEIGHBOURS) {
      hitsByNeighbour = truncateHitsToTop(rawHits, MAX_OWNER_LOOKUP_NEIGHBOURS);
      isPartial = true;
    } else {
      hitsByNeighbour = rawHits;
    }
    final Map<Urn, Set<Urn>> ownersByNeighbour =
        ownerStrategy.resolveOwners(context.getOperationContext(), hitsByNeighbour.keySet());

    final Map<Urn, OwnerBucket> bucketsByOwner = new HashMap<>();
    for (final Map.Entry<Urn, LineageHit> entry : hitsByNeighbour.entrySet()) {
      final Urn neighbourUrn = entry.getKey();
      final LineageHit hit = entry.getValue();
      final Set<Urn> owners = ownersByNeighbour.get(neighbourUrn);
      if (owners == null || owners.isEmpty()) {
        // Drop neighbours we couldn't map to an owner; mark the response partial so the UI can
        // surface a "results incomplete" hint.
        isPartial = true;
        continue;
      }
      for (final Urn ownerUrn : owners) {
        if (ownerUrn.equals(request.getSourceUrn())) {
          continue;
        }
        final OwnerBucket bucket = bucketsByOwner.computeIfAbsent(ownerUrn, OwnerBucket::new);
        bucket.memberMatches.addAll(hit.contributingMembers);
        bucket.neighbourEntities.add(neighbourUrn);
        bucket.degreeMin = Math.min(bucket.degreeMin, hit.minDegree);
        bucket.degreeMax = Math.max(bucket.degreeMax, hit.maxDegree);
      }
    }

    final Map<Urn, OwnerBucket> finalBuckets =
        applyAuthorization(context.getOperationContext(), request, bucketsByOwner);

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
    final List<AggregatedLineageResponse.Relationship> relationships =
        sorted.subList(from, to).stream()
            .map(b -> toRelationship(context, b))
            .collect(Collectors.toList());

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

  /**
   * Fan out {@code searchAcrossLineage} across all members, in fixed-size batches so we never have
   * more than {@link #MAX_PARALLEL_FANOUT} simultaneous tasks queued on the shared GraphQL executor
   * (which would starve other GraphQL requests and slam Elasticsearch).
   */
  private List<LineageSearchResult> fanOutSearchAcrossLineage(
      final QueryContext context, final AggregatedLineageRequest request, final List<Urn> members) {
    final OperationContext opContext = applyFlags(context.getOperationContext(), request);
    final com.linkedin.metadata.query.filter.Filter filter =
        ResolverUtils.buildFilter(null, request.getOrFilters());
    final com.linkedin.metadata.graph.LineageDirection internalDirection =
        com.linkedin.metadata.graph.LineageDirection.valueOf(request.getDirection().name());
    final List<String> entityNames = getNeighbourEntityTypes();
    final ExecutorService executor = GraphQLConcurrencyUtils.getExecutorService();

    final List<LineageSearchResult> results = new ArrayList<>(members.size());
    for (int batchStart = 0; batchStart < members.size(); batchStart += MAX_PARALLEL_FANOUT) {
      final List<Urn> batch =
          members.subList(batchStart, Math.min(batchStart + MAX_PARALLEL_FANOUT, members.size()));
      final List<CompletableFuture<LineageSearchResult>> futures =
          batch.stream()
              .map(
                  memberUrn -> {
                    final java.util.function.Supplier<LineageSearchResult> task =
                        () ->
                            searchLineageForMember(
                                opContext,
                                memberUrn,
                                internalDirection,
                                entityNames,
                                filter,
                                request);
                    return executor != null
                        ? CompletableFuture.supplyAsync(task, executor)
                        : CompletableFuture.supplyAsync(task);
                  })
              .collect(Collectors.toList());
      futures.forEach(f -> results.add(f.join()));
    }
    return results;
  }

  private LineageSearchResult searchLineageForMember(
      final OperationContext opContext,
      final Urn memberUrn,
      final com.linkedin.metadata.graph.LineageDirection direction,
      final List<String> entityNames,
      final com.linkedin.metadata.query.filter.Filter filter,
      final AggregatedLineageRequest request) {
    try {
      return entityClient.searchAcrossLineage(
          opContext,
          memberUrn,
          direction,
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

  /**
   * Keep the {@code cap} neighbours with the most contributing members; drop the rest. The dropped
   * ones contribute fewer paths and would rank lower in the final sort anyway, so this is a
   * loss-of-tail rather than a loss-of-head. Caller flips {@code isPartial} when this fires.
   */
  private static Map<Urn, LineageHit> truncateHitsToTop(
      final Map<Urn, LineageHit> hits, final int cap) {
    final List<Map.Entry<Urn, LineageHit>> sorted = new ArrayList<>(hits.entrySet());
    sorted.sort(
        Comparator.<Map.Entry<Urn, LineageHit>>comparingInt(
                e -> e.getValue().contributingMembers.size())
            .reversed()
            .thenComparing(e -> e.getKey().toString()));
    final Map<Urn, LineageHit> capped = new LinkedHashMap<>(cap);
    for (int i = 0; i < cap && i < sorted.size(); i++) {
      capped.put(sorted.get(i).getKey(), sorted.get(i).getValue());
    }
    return capped;
  }

  private static Map<Urn, LineageHit> collectHits(
      final List<Urn> members, final List<LineageSearchResult> perMemberResults) {
    final Set<Urn> memberSet = new HashSet<>(members);
    final Map<Urn, LineageHit> byNeighbour = new HashMap<>();
    for (int i = 0; i < members.size(); i++) {
      final Urn memberUrn = members.get(i);
      final LineageSearchResult result = perMemberResults.get(i);
      if (result == null || result.getEntities() == null) {
        continue;
      }
      for (final LineageSearchEntity entity : result.getEntities()) {
        final Urn neighbourUrn = entity.getEntity();
        if (neighbourUrn == null || memberSet.contains(neighbourUrn)) {
          continue;
        }
        final int degree =
            entity.getDegrees() != null && !entity.getDegrees().isEmpty()
                ? entity.getDegrees().stream().mapToInt(Integer::intValue).min().orElse(1)
                : 1;
        final LineageHit hit = byNeighbour.computeIfAbsent(neighbourUrn, k -> new LineageHit());
        hit.contributingMembers.add(memberUrn);
        hit.minDegree = Math.min(hit.minDegree, degree);
        hit.maxDegree = Math.max(hit.maxDegree, degree);
      }
    }
    return byNeighbour;
  }

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
    }
    return out;
  }

  private AggregatedLineageResponse.Relationship toRelationship(
      final QueryContext context, final OwnerBucket bucket) {
    return AggregatedLineageResponse.Relationship.builder()
        .entity(buildEntity(context, bucket.ownerUrn))
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
    // Unknown entity type: fall back to a Restricted placeholder so the non-null schema
    // contract still holds and the frontend renders conservatively.
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

  private static final class LineageHit {
    final Set<Urn> contributingMembers = new HashSet<>();
    int minDegree = Integer.MAX_VALUE;
    int maxDegree = Integer.MIN_VALUE;
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

    OwnerBucket rekey(final Urn newOwnerUrn) {
      final OwnerBucket rekeyed = new OwnerBucket(newOwnerUrn);
      rekeyed.memberMatches.addAll(this.memberMatches);
      rekeyed.neighbourEntities.addAll(this.neighbourEntities);
      rekeyed.degreeMin = this.degreeMin;
      rekeyed.degreeMax = this.degreeMax;
      return rekeyed;
    }
  }
}
