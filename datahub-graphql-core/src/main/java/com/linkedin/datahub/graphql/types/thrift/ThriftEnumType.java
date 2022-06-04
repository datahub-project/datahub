package com.linkedin.datahub.graphql.types.thrift;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.ThriftEnum;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.ThriftEnumUtils;
import com.linkedin.datahub.graphql.types.mappers.BrowsePathsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowseResultMapper;
import com.linkedin.datahub.graphql.types.thrift.mapper.ThriftEnumMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.browse.BrowseResult;

import graphql.com.google.common.collect.ImmutableSet;
import graphql.execution.DataFetcherResult;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.datahub.graphql.Constants.*;

public class ThriftEnumType implements BrowsableEntityType<ThriftEnum, String> {

  private static final Set<String> ASPECTS_TO_RESOLVE = ImmutableSet.of(
    THRIFT_ENUM_KEY_ASPECT_NAME,
    THRIFT_ENUM_PROPERTIES_ASPECT_NAME
  );

  private static final Set<String> FACET_FIELDS = ImmutableSet.of();

  private static final String ENTITY_NAME = "thriftEnum";

  private final EntityClient _entityClient;

  public ThriftEnumType(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public EntityType type() {
    return EntityType.THRIFT_ENUM;
  }

  @Override
  public Class<ThriftEnum> objectClass() {
    return ThriftEnum.class;
  }

  @Override
  public List<DataFetcherResult<ThriftEnum>> batchLoad(
    List<String> urnStrs,
    QueryContext context
  ) {
    final List<Urn> urns = urnStrs
      .stream()
      .map(UrnUtils::getUrn)
      .collect(Collectors.toList());
    try {
      final Map<Urn, EntityResponse> pinterestThriftEnumItemMap = _entityClient.batchGetV2(
        Constants.THRIFT_ENUM_ENTITY_NAME,
        new HashSet<>(urns),
        ASPECTS_TO_RESOLVE,
        context.getAuthentication()
      );

      final List<EntityResponse> gmsResults = new ArrayList<>();
      for (Urn urn : urns) {
        gmsResults.add(pinterestThriftEnumItemMap.getOrDefault(urn, null));
      }
      return gmsResults
        .stream()
        .map(
          gmsThriftEnum ->
            gmsThriftEnum == null
              ? null
              : DataFetcherResult
                .<ThriftEnum>newResult()
                .data(ThriftEnumMapper.map(gmsThriftEnum))
                .build()
        )
        .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException(
        "Failed to batch load PinterestThriftEnumItems",
        e
      );
    }
  }

  @Override
  public BrowseResults browse(@Nonnull List<String> path,
                              @Nullable List<FacetFilterInput> filters,
                              int start,
                              int count,
                              @Nonnull final QueryContext context) throws Exception {
      final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
      final String pathStr = path.size() > 0 ? BROWSE_PATH_DELIMITER + String.join(BROWSE_PATH_DELIMITER, path) : "";
      final BrowseResult result = _entityClient.browse(
              "thriftEnum",
              pathStr,
              facetFilters,
              start,
              count,
          context.getAuthentication());
      return BrowseResultMapper.map(result);
  }

  @Override
  public List<BrowsePath> browsePaths(@Nonnull String urn, @Nonnull final QueryContext context) throws Exception {
      final StringArray result = _entityClient.getBrowsePaths(ThriftEnumUtils.getThriftEnumUrn(urn), context.getAuthentication());
      return BrowsePathsMapper.map(result);
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }
}
