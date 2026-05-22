package com.linkedin.datahub.graphql.resolvers.dataproduct;

import com.datahub.authorization.AuthorizationConfiguration;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataProduct;
import com.linkedin.datahub.graphql.generated.DataProductLineageInput;
import com.linkedin.datahub.graphql.generated.DataProductLineageRelationship;
import com.linkedin.datahub.graphql.generated.DataProductLineageResult;
import com.linkedin.datahub.graphql.resolvers.lineage.aggregate.AggregatedLineageRequest;
import com.linkedin.datahub.graphql.resolvers.lineage.aggregate.AggregatedLineageResolver;
import com.linkedin.datahub.graphql.resolvers.lineage.aggregate.AggregatedLineageResponse;
import com.linkedin.datahub.graphql.resolvers.lineage.aggregate.DataProductOwnerResolutionStrategy;
import com.linkedin.datahub.graphql.resolvers.lineage.aggregate.MembersResult;
import com.linkedin.datahub.graphql.resolvers.lineage.aggregate.OwnerResolutionStrategy;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.service.DataProductService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.services.RestrictedService;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver for {@code DataProduct.dataProductLineage}. Members come from {@code
 * DataProductProperties.assets}; owners are always resolved via {@link
 * DataProductOwnerResolutionStrategy}.
 */
@Slf4j
public class DataProductLineageResolver
    extends AggregatedLineageResolver<DataProductLineageInput, DataProductLineageResult> {

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

  private final DataProductService dataProductService;
  private final DataProductOwnerResolutionStrategy ownerStrategy;

  public DataProductLineageResolver(
      final EntityClient entityClient,
      final GraphClient graphClient,
      final DataProductService dataProductService,
      final RestrictedService restrictedService,
      final AuthorizationConfiguration authorizationConfiguration) {
    super(entityClient, restrictedService, authorizationConfiguration);
    this.dataProductService = dataProductService;
    this.ownerStrategy = new DataProductOwnerResolutionStrategy(graphClient);
  }

  @Override
  protected Class<DataProductLineageInput> getInputClass() {
    return DataProductLineageInput.class;
  }

  @Override
  protected Urn extractSourceUrn(final DataFetchingEnvironment environment) {
    return UrnUtils.getUrn(((DataProduct) environment.getSource()).getUrn());
  }

  @Override
  protected AggregatedLineageRequest toCanonicalRequest(
      final DataProductLineageInput input, final Urn sourceUrn) {
    return AggregatedLineageRequest.builder()
        .sourceUrn(sourceUrn)
        .direction(input.getDirection())
        .hops(input.getHops() != null ? input.getHops() : 1)
        .start(input.getStart() != null ? input.getStart() : 0)
        .count(input.getCount() != null ? input.getCount() : 25)
        .perMemberCount(input.getPerMemberCount() != null ? input.getPerMemberCount() : 25)
        .memberScanCap(input.getMemberScanCap() != null ? input.getMemberScanCap() : 300)
        .groupByDataProduct(false)
        .includeRestricted(input.getIncludeRestricted() == null || input.getIncludeRestricted())
        .searchFlags(input.getSearchFlags())
        .lineageFlags(input.getLineageFlags())
        .orFilters(input.getOrFilters())
        .build();
  }

  @Override
  protected MembersResult enumerateMembers(
      final QueryContext context, final Urn sourceUrn, final int memberScanCap) {
    try {
      final DataProductProperties props =
          dataProductService.getDataProductProperties(context.getOperationContext(), sourceUrn);
      if (props == null || !props.hasAssets() || props.getAssets() == null) {
        return new MembersResult(Collections.emptyList(), 0);
      }
      final List<Urn> allMembers =
          props.getAssets().stream()
              .map(DataProductAssociation::getDestinationUrn)
              .collect(Collectors.toList());
      final int total = allMembers.size();
      final List<Urn> capped =
          total > memberScanCap ? allMembers.subList(0, memberScanCap) : allMembers;
      return new MembersResult(capped, total);
    } catch (Exception e) {
      log.warn(
          "Failed to enumerate DataProduct members for {} (cap={}); returning empty member set.",
          sourceUrn,
          memberScanCap,
          e);
      return new MembersResult(Collections.emptyList(), 0);
    }
  }

  @Override
  protected OwnerResolutionStrategy resolveOwnerStrategy(
      final DataProductLineageInput input, final AggregatedLineageRequest request) {
    return ownerStrategy;
  }

  @Override
  protected List<String> getNeighbourEntityTypes() {
    return LINEAGE_BEARING_TYPES.stream()
        .map(EntityTypeMapper::getName)
        .collect(Collectors.toList());
  }

  @Override
  protected DataProductLineageResult buildResult(final AggregatedLineageResponse response) {
    final DataProductLineageResult result = new DataProductLineageResult();
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

  private DataProductLineageRelationship toGraphQlRelationship(
      final AggregatedLineageResponse.Relationship rel, final AggregatedLineageResponse response) {
    final DataProductLineageRelationship out = new DataProductLineageRelationship();
    out.setEntity(rel.getEntity());
    out.setDirection(response.getDirection());
    out.setMemberMatchCount(rel.getMemberMatchCount());
    out.setNeighbourEntityCount(rel.getNeighbourEntityCount());
    out.setDegreeMin(rel.getDegreeMin());
    out.setDegreeMax(rel.getDegreeMax());
    return out;
  }
}
