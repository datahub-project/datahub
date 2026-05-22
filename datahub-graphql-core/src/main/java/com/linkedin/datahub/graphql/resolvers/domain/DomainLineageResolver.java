package com.linkedin.datahub.graphql.resolvers.domain;

import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.datahub.authorization.AuthorizationConfiguration;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.DomainLineageInput;
import com.linkedin.datahub.graphql.generated.DomainLineageRelationship;
import com.linkedin.datahub.graphql.generated.DomainLineageResult;
import com.linkedin.datahub.graphql.resolvers.lineage.aggregate.AggregatedLineageRequest;
import com.linkedin.datahub.graphql.resolvers.lineage.aggregate.AggregatedLineageResolver;
import com.linkedin.datahub.graphql.resolvers.lineage.aggregate.AggregatedLineageResponse;
import com.linkedin.datahub.graphql.resolvers.lineage.aggregate.DataProductOwnerResolutionStrategy;
import com.linkedin.datahub.graphql.resolvers.lineage.aggregate.DomainOwnerResolutionStrategy;
import com.linkedin.datahub.graphql.resolvers.lineage.aggregate.MembersResult;
import com.linkedin.datahub.graphql.resolvers.lineage.aggregate.OwnerResolutionStrategy;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.services.RestrictedService;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver for {@code Domain.domainLineage}.
 *
 * <p>Members are enumerated by searching all lineage-bearing assets whose indexed {@code domains}
 * field exactly matches the source Domain URN. Owners are resolved by either the {@link
 * DomainOwnerResolutionStrategy} (default) or {@link DataProductOwnerResolutionStrategy} when the
 * input flag {@code groupByDataProduct=true} is set.
 */
@Slf4j
public class DomainLineageResolver
    extends AggregatedLineageResolver<DomainLineageInput, DomainLineageResult> {

  private static final String DOMAINS_FIELD = "domains";

  // Lineage-bearing entity types (excludes governance entities); mirrors impact-analysis.
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

  private final DomainOwnerResolutionStrategy domainStrategy;
  private final DataProductOwnerResolutionStrategy dataProductStrategy;

  public DomainLineageResolver(
      final EntityClient entityClient,
      final GraphClient graphClient,
      final RestrictedService restrictedService,
      final AuthorizationConfiguration authorizationConfiguration) {
    super(entityClient, restrictedService, authorizationConfiguration);
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
          "Failed to enumerate Domain members for {} (cap={}); returning empty member set.",
          sourceUrn,
          memberScanCap,
          e);
      return new MembersResult(Collections.emptyList(), 0);
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
    return result;
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
}
