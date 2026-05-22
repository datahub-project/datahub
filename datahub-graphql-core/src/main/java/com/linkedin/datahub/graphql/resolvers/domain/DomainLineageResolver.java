package com.linkedin.datahub.graphql.resolvers.domain;

import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.datahub.authorization.AuthorizationConfiguration;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.DomainAggregatedInnerEdge;
import com.linkedin.datahub.graphql.generated.DomainLineageInput;
import com.linkedin.datahub.graphql.generated.DomainLineageRelationship;
import com.linkedin.datahub.graphql.generated.DomainLineageResult;
import com.linkedin.datahub.graphql.generated.LineageDirection;
import com.linkedin.datahub.graphql.resolvers.lineage.aggregate.AggregatedLineageRequest;
import com.linkedin.datahub.graphql.resolvers.lineage.aggregate.AggregatedLineageResolver;
import com.linkedin.datahub.graphql.resolvers.lineage.aggregate.AggregatedLineageResponse;
import com.linkedin.datahub.graphql.resolvers.lineage.aggregate.DataProductOwnerResolutionStrategy;
import com.linkedin.datahub.graphql.resolvers.lineage.aggregate.DomainOwnerResolutionStrategy;
import com.linkedin.datahub.graphql.resolvers.lineage.aggregate.MembersResult;
import com.linkedin.datahub.graphql.resolvers.lineage.aggregate.OwnerResolutionStrategy;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.RestrictedService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver for {@code Domain.domainLineage}. Members are enumerated as the union of (a) assets
 * tagged with the source Domain directly and (b) assets reachable via {@code DataProductContains}
 * from a DataProduct in the source Domain — governance teams typically tag DPs and leave the
 * underlying assets untagged, so (b) is the common case. Owners are resolved by the Domain strategy
 * (default) or the DataProduct strategy when {@code groupByDataProduct=true}.
 */
@Slf4j
public class DomainLineageResolver
    extends AggregatedLineageResolver<DomainLineageInput, DomainLineageResult> {

  private static final String DOMAINS_FIELD = "domains";
  private static final Set<String> DATA_PRODUCT_CONTAINS_REL =
      Collections.singleton("DataProductContains");

  // Caps DataProduct enumeration in the source Domain (used for inner-edge buckets and transitive
  // member expansion). Membership beyond this is silently ignored.
  private static final int INNER_EDGE_DP_ENUMERATION_CAP = 1_000;

  // Per-DP cap on assets pulled via DataProductContains. Total transitive enumeration is also
  // bounded by memberScanCap across all DPs.
  private static final int MAX_MEMBERS_PER_DP_ENUMERATION = 500;

  // Lineage-bearing entity types; mirrors impact-analysis.
  private static final List<com.linkedin.datahub.graphql.generated.EntityType>
      LINEAGE_BEARING_TYPES =
          List.of(
              com.linkedin.datahub.graphql.generated.EntityType.DATASET,
              com.linkedin.datahub.graphql.generated.EntityType.DASHBOARD,
              com.linkedin.datahub.graphql.generated.EntityType.CHART,
              com.linkedin.datahub.graphql.generated.EntityType.MLMODEL,
              com.linkedin.datahub.graphql.generated.EntityType.MLMODEL_GROUP,
              com.linkedin.datahub.graphql.generated.EntityType.MLFEATURE_TABLE,
              com.linkedin.datahub.graphql.generated.EntityType.MLFEATURE,
              com.linkedin.datahub.graphql.generated.EntityType.MLPRIMARY_KEY,
              com.linkedin.datahub.graphql.generated.EntityType.DATA_FLOW,
              com.linkedin.datahub.graphql.generated.EntityType.DATA_JOB,
              com.linkedin.datahub.graphql.generated.EntityType.CONTAINER,
              com.linkedin.datahub.graphql.generated.EntityType.NOTEBOOK);

  private final GraphClient graphClient;
  private final DomainOwnerResolutionStrategy domainStrategy;
  private final DataProductOwnerResolutionStrategy dataProductStrategy;

  public DomainLineageResolver(
      final EntityClient entityClient,
      final GraphClient graphClient,
      final RestrictedService restrictedService,
      final AuthorizationConfiguration authorizationConfiguration) {
    super(entityClient, restrictedService, authorizationConfiguration);
    this.graphClient = graphClient;
    this.domainStrategy = new DomainOwnerResolutionStrategy(entityClient);
    this.dataProductStrategy = new DataProductOwnerResolutionStrategy(graphClient);
  }

  @Override
  protected Class<DomainLineageInput> getInputClass() {
    return DomainLineageInput.class;
  }

  @Override
  protected Urn extractSourceUrn(final DataFetchingEnvironment environment) {
    return UrnUtils.getUrn(((Domain) environment.getSource()).getUrn());
  }

  @Override
  protected AggregatedLineageRequest toCanonicalRequest(
      final DomainLineageInput input, final Urn sourceUrn) {
    return AggregatedLineageRequest.builder()
        .sourceUrn(sourceUrn)
        .direction(input.getDirection())
        .hops(input.getHops() != null ? input.getHops() : 1)
        .start(input.getStart() != null ? input.getStart() : 0)
        .count(input.getCount() != null ? input.getCount() : 25)
        .perMemberCount(input.getPerMemberCount() != null ? input.getPerMemberCount() : 25)
        .memberScanCap(input.getMemberScanCap() != null ? input.getMemberScanCap() : 300)
        .groupByDataProduct(input.getGroupByDataProduct() != null && input.getGroupByDataProduct())
        .includeRestricted(input.getIncludeRestricted() == null || input.getIncludeRestricted())
        .searchFlags(input.getSearchFlags())
        .lineageFlags(input.getLineageFlags())
        .orFilters(input.getOrFilters())
        .build();
  }

  @Override
  protected MembersResult enumerateMembers(
      final QueryContext context, final Urn sourceUrn, final int memberScanCap) {
    final MembersResult direct = enumerateDirectMembers(context, sourceUrn, memberScanCap);
    final Set<Urn> dpsInDomain = enumerateDataProductsInDomain(context, sourceUrn);
    final TransitiveMembers transitive =
        collectAssetsFromDataProducts(context, dpsInDomain, memberScanCap);

    final LinkedHashSet<Urn> union = new LinkedHashSet<>(direct.getUrns());
    union.addAll(transitive.urns);
    final List<Urn> cappedUrns = union.stream().limit(memberScanCap).collect(Collectors.toList());

    // We can't cheaply compute an exact deduped total across two enumeration paths; if either
    // signalled more-available, lift total above the cap so the wrapper flips isPartial=true.
    final boolean directHasMore = direct.getTotal() > direct.getUrns().size();
    final int total;
    if (directHasMore || transitive.truncated) {
      total = Math.max(union.size() + 1, direct.getTotal());
    } else {
      total = union.size();
    }

    return new MembersResult(cappedUrns, total);
  }

  private MembersResult enumerateDirectMembers(
      final QueryContext context, final Urn sourceUrn, final int memberScanCap) {
    final Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                buildCriterion(
                                    DOMAINS_FIELD + ".keyword",
                                    Condition.EQUAL,
                                    sourceUrn.toString())))));
    try {
      final SearchResult result =
          entityClient.searchAcrossEntities(
              context.getOperationContext(),
              getNeighbourEntityTypes(),
              "*",
              filter,
              0,
              memberScanCap,
              Collections.emptyList());
      final List<Urn> urns =
          result.getEntities() == null
              ? Collections.emptyList()
              : result.getEntities().stream().map(e -> e.getEntity()).collect(Collectors.toList());
      final int total = result.getNumEntities() != null ? result.getNumEntities() : urns.size();
      return new MembersResult(urns, total);
    } catch (Exception e) {
      log.warn(
          "Failed to enumerate direct Domain members for {} (cap={}); falling back to empty.",
          sourceUrn,
          memberScanCap,
          e);
      return new MembersResult(Collections.emptyList(), 0);
    }
  }

  /**
   * Collects lineage-bearing assets from every DP in {@code dpUrns} via {@code
   * DataProductContains}. Bounded by {@code memberScanCap} across all DPs and {@link
   * #MAX_MEMBERS_PER_DP_ENUMERATION} per DP.
   */
  private TransitiveMembers collectAssetsFromDataProducts(
      final QueryContext context, final Set<Urn> dpUrns, final int memberScanCap) {
    if (dpUrns.isEmpty()) {
      return new TransitiveMembers(Collections.emptyList(), false);
    }
    final Set<String> lineageBearingTypeNames =
        LINEAGE_BEARING_TYPES.stream()
            .map(EntityTypeMapper::getName)
            .collect(Collectors.toUnmodifiableSet());
    final String actorUrn =
        context.getOperationContext().getSessionActorContext().getActorUrn().toString();
    final LinkedHashSet<Urn> assets = new LinkedHashSet<>();
    boolean truncated = false;

    for (final Urn dpUrn : dpUrns) {
      if (assets.size() >= memberScanCap) {
        truncated = true;
        break;
      }
      final int remaining = memberScanCap - assets.size();
      try {
        final EntityRelationships rels =
            graphClient.getRelatedEntities(
                dpUrn.toString(),
                DATA_PRODUCT_CONTAINS_REL,
                RelationshipDirection.OUTGOING,
                0,
                Math.min(remaining, MAX_MEMBERS_PER_DP_ENUMERATION),
                actorUrn);
        if (rels == null || rels.getRelationships() == null) {
          continue;
        }
        for (final EntityRelationship r : rels.getRelationships()) {
          final Urn assetUrn = r.getEntity();
          if (assetUrn == null || !lineageBearingTypeNames.contains(assetUrn.getEntityType())) {
            continue;
          }
          assets.add(assetUrn);
          if (assets.size() >= memberScanCap) {
            truncated = true;
            break;
          }
        }
      } catch (Exception e) {
        log.warn(
            "Failed to enumerate assets contained in DataProduct {}; skipping that DP.", dpUrn, e);
      }
    }
    return new TransitiveMembers(new ArrayList<>(assets), truncated);
  }

  private static final class TransitiveMembers {
    final List<Urn> urns;
    final boolean truncated;

    TransitiveMembers(final List<Urn> urns, final boolean truncated) {
      this.urns = urns;
      this.truncated = truncated;
    }
  }

  @Override
  protected OwnerResolutionStrategy resolveOwnerStrategy(
      final DomainLineageInput input, final AggregatedLineageRequest request) {
    return request.isGroupByDataProduct() ? dataProductStrategy : domainStrategy;
  }

  @Override
  protected List<String> getNeighbourEntityTypes() {
    return LINEAGE_BEARING_TYPES.stream()
        .map(EntityTypeMapper::getName)
        .collect(Collectors.toList());
  }

  @Override
  protected DomainLineageResult buildResult(final AggregatedLineageResponse response) {
    final DomainLineageResult result = new DomainLineageResult();
    result.setStart(response.getStart());
    result.setCount(response.getCount());
    result.setTotal(response.getTotal());
    result.setIsPartial(response.isPartial());
    result.setMemberScanCount(response.getMemberScanCount());
    result.setMemberTotal(response.getMemberTotal());
    result.setRelationships(
        response.getRelationships().stream()
            .map(rel -> toGraphQlRelationship(rel, response))
            .collect(Collectors.toList()));
    result.setInnerEdges(
        response.getInnerEdges().stream()
            .map(DomainLineageResolver::toGraphQlInnerEdge)
            .collect(Collectors.toList()));
    return result;
  }

  /**
   * Surfaces DP↔DP edges where both DPs belong to the source Domain. Looks up DP membership for
   * every contributing member and within-scope neighbour, then emits a bucket per {@code
   * (upstreamDp, downstreamDp)} pair. The frontend dedupes across the two direction queries.
   */
  @Override
  protected List<AggregatedLineageResponse.InnerEdge> computeInnerEdges(
      final QueryContext context,
      final AggregatedLineageRequest request,
      final MembersResult members,
      final Map<Urn, AggregatedLineageResolver.LineageHit> withinScopeHits) {

    if (members.getUrns().isEmpty() || withinScopeHits.isEmpty()) {
      return Collections.emptyList();
    }

    final OperationContext opContext = context.getOperationContext();
    final Set<Urn> sourceDomainDps = enumerateDataProductsInDomain(context, request.getSourceUrn());
    if (sourceDomainDps.isEmpty()) {
      return Collections.emptyList();
    }

    final Map<Urn, Set<Urn>> dpsByAsset =
        dataProductStrategy.resolveOwners(opContext, new HashSet<>(members.getUrns()));

    final Map<EdgeKey, EdgeBucket> bucketsByEdge = new HashMap<>();
    final boolean upstreamQuery = request.getDirection() == LineageDirection.UPSTREAM;
    for (final Map.Entry<Urn, AggregatedLineageResolver.LineageHit> entry :
        withinScopeHits.entrySet()) {
      final Urn neighbourUrn = entry.getKey();
      final Set<Urn> neighbourDps = intersect(dpsByAsset.get(neighbourUrn), sourceDomainDps);
      if (neighbourDps.isEmpty()) {
        continue;
      }
      final AggregatedLineageResolver.LineageHit hit = entry.getValue();
      for (final Urn memberUrn : hit.contributingMembers) {
        final Set<Urn> memberDps = intersect(dpsByAsset.get(memberUrn), sourceDomainDps);
        if (memberDps.isEmpty()) {
          continue;
        }
        for (final Urn memberDp : memberDps) {
          for (final Urn neighbourDp : neighbourDps) {
            if (memberDp.equals(neighbourDp)) {
              continue;
            }
            final Urn upstreamDp = upstreamQuery ? neighbourDp : memberDp;
            final Urn downstreamDp = upstreamQuery ? memberDp : neighbourDp;
            final EdgeKey key = new EdgeKey(upstreamDp, downstreamDp);
            final EdgeBucket bucket = bucketsByEdge.computeIfAbsent(key, k -> new EdgeBucket());
            bucket.pairCount++;
            bucket.degreeMin = Math.min(bucket.degreeMin, hit.minDegree);
            bucket.degreeMax = Math.max(bucket.degreeMax, hit.maxDegree);
          }
        }
      }
    }

    if (bucketsByEdge.isEmpty()) {
      return Collections.emptyList();
    }

    final List<AggregatedLineageResponse.InnerEdge> out = new ArrayList<>(bucketsByEdge.size());
    bucketsByEdge.forEach(
        (key, bucket) ->
            out.add(
                AggregatedLineageResponse.InnerEdge.builder()
                    .upstream(buildEntity(context, key.upstream))
                    .downstream(buildEntity(context, key.downstream))
                    .memberMatchCount(bucket.pairCount)
                    .degreeMin(bucket.degreeMin)
                    .degreeMax(bucket.degreeMax)
                    .build()));
    return out;
  }

  private Set<Urn> enumerateDataProductsInDomain(
      final QueryContext context, final Urn sourceDomainUrn) {
    final Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                buildCriterion(
                                    DOMAINS_FIELD + ".keyword",
                                    Condition.EQUAL,
                                    sourceDomainUrn.toString())))));
    try {
      final SearchResult result =
          entityClient.searchAcrossEntities(
              context.getOperationContext(),
              List.of(Constants.DATA_PRODUCT_ENTITY_NAME),
              "*",
              filter,
              0,
              INNER_EDGE_DP_ENUMERATION_CAP,
              Collections.emptyList());
      if (result.getEntities() == null) {
        return Collections.emptySet();
      }
      return result.getEntities().stream()
          .map(e -> e.getEntity())
          .filter(Objects::nonNull)
          .collect(Collectors.toCollection(HashSet::new));
    } catch (Exception e) {
      log.warn(
          "Failed to enumerate DataProducts in Domain {}; skipping inner-edge computation.",
          sourceDomainUrn,
          e);
      return Collections.emptySet();
    }
  }

  private static Set<Urn> intersect(final Set<Urn> a, final Set<Urn> b) {
    if (a == null || a.isEmpty() || b == null || b.isEmpty()) {
      return Collections.emptySet();
    }
    final Set<Urn> out = new HashSet<>(Math.min(a.size(), b.size()));
    for (final Urn u : a) {
      if (b.contains(u)) {
        out.add(u);
      }
    }
    return out;
  }

  private DomainLineageRelationship toGraphQlRelationship(
      final AggregatedLineageResponse.Relationship rel, final AggregatedLineageResponse response) {
    final DomainLineageRelationship out = new DomainLineageRelationship();
    out.setEntity(rel.getEntity());
    out.setDirection(response.getDirection());
    out.setMemberMatchCount(rel.getMemberMatchCount());
    out.setNeighbourEntityCount(rel.getNeighbourEntityCount());
    out.setDegreeMin(rel.getDegreeMin());
    out.setDegreeMax(rel.getDegreeMax());
    return out;
  }

  private static DomainAggregatedInnerEdge toGraphQlInnerEdge(
      final AggregatedLineageResponse.InnerEdge edge) {
    final DomainAggregatedInnerEdge out = new DomainAggregatedInnerEdge();
    out.setUpstream(edge.getUpstream());
    out.setDownstream(edge.getDownstream());
    out.setMemberMatchCount(edge.getMemberMatchCount());
    out.setDegreeMin(edge.getDegreeMin());
    out.setDegreeMax(edge.getDegreeMax());
    return out;
  }

  private static final class EdgeKey {
    final Urn upstream;
    final Urn downstream;

    EdgeKey(final Urn upstream, final Urn downstream) {
      this.upstream = upstream;
      this.downstream = downstream;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof EdgeKey)) return false;
      final EdgeKey other = (EdgeKey) o;
      return upstream.equals(other.upstream) && downstream.equals(other.downstream);
    }

    @Override
    public int hashCode() {
      return Objects.hash(upstream, downstream);
    }
  }

  private static final class EdgeBucket {
    int pairCount = 0;
    int degreeMin = Integer.MAX_VALUE;
    int degreeMax = Integer.MIN_VALUE;
  }
}
