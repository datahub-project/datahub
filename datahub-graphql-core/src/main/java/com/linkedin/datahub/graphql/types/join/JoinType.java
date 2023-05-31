package com.linkedin.datahub.graphql.types.join;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.JoinUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.Join;
import com.linkedin.datahub.graphql.generated.JoinUpdateInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.MutableType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.join.mappers.JoinMapper;
import com.linkedin.datahub.graphql.types.join.mappers.JoinUpdateInputMapper;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowsePathsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowseResultMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.datahub.graphql.Constants.*;
import static com.linkedin.metadata.Constants.*;


public class JoinType implements com.linkedin.datahub.graphql.types.EntityType<Join, String>,
                                 BrowsableEntityType<Join, String>, SearchableEntityType<Join, String>,
                                 MutableType<JoinUpdateInput, Join> {


  static final Set<String> ASPECTS_TO_RESOLVE = ImmutableSet.of(
      JOIN_KEY_ASPECT_NAME,
      JOIN_PROPERTIES_ASPECT_NAME,
      EDITABLE_JOIN_PROPERTIES_ASPECT_NAME,
      INSTITUTIONAL_MEMORY_ASPECT_NAME,
      OWNERSHIP_ASPECT_NAME,
      STATUS_ASPECT_NAME,
      CONTAINER_ASPECT_NAME,
      GLOBAL_TAGS_ASPECT_NAME,
      GLOSSARY_TERMS_ASPECT_NAME,
      BROWSE_PATHS_ASPECT_NAME
  );

  private static final Set<String> FACET_FIELDS = ImmutableSet.of("name");
  private static final String ENTITY_NAME = "join";

  private final EntityClient _entityClient;


  public JoinType(final EntityClient entityClient)  {
    _entityClient = entityClient;
  }

  @Override
  public Class<Join> objectClass() {
    return Join.class;
  }


  @Override
  public Class<JoinUpdateInput> inputClass() {
    return JoinUpdateInput.class;
  }

  @Override
  public EntityType type() {
    return EntityType.JOIN;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public List<DataFetcherResult<Join>> batchLoad(@Nonnull final List<String> urns, @Nonnull final QueryContext context)
      throws Exception {
    final List<Urn> joinUrns = urns.stream()
        .map(this::getUrn)
        .collect(Collectors.toList());

    try {
      final Map<Urn, EntityResponse> entities = _entityClient.batchGetV2(
          JOIN_ENTITY_NAME,
          new HashSet<>(joinUrns),
          ASPECTS_TO_RESOLVE,
          context.getAuthentication());

      final List<EntityResponse> gmsResults = new ArrayList<>();
      for (Urn urn : joinUrns) {
        gmsResults.add(entities.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(gmsResult ->
              gmsResult == null ? null : DataFetcherResult.<Join>newResult()
                  .data(JoinMapper.map(gmsResult))
                  .build()
          )
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to load join entity", e);
    }
  }

  @Nonnull
  @Override
  public BrowseResults browse(@Nonnull List<String> path, @Nullable List<FacetFilterInput> filters, int start,
      int count, @Nonnull QueryContext context) throws Exception {
    final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
    final String pathStr = path.size() > 0 ? BROWSE_PATH_DELIMITER + String.join(BROWSE_PATH_DELIMITER, path) : "";
    final BrowseResult result = _entityClient.browse(
        "join",
        pathStr,
        facetFilters,
        start,
        count,
        context.getAuthentication());
    return BrowseResultMapper.map(result);
  }

  private Urn getUrn(final String urnStr) {
    try {
      return Urn.createFromString(urnStr);
    } catch (URISyntaxException e) {
      throw new RuntimeException(String.format("Failed to convert urn string %s into Urn", urnStr));
    }
  }



  @Nonnull
  @Override
  public List<BrowsePath> browsePaths(@Nonnull String urn, @Nonnull QueryContext context) throws Exception {
    final StringArray result = _entityClient.getBrowsePaths(getJoinUrn(urn), context.getAuthentication());
    return BrowsePathsMapper.map(result);
  }

  private com.linkedin.common.urn.JoinUrn getJoinUrn(String urnStr) {
    try {
      return JoinUrn.createFromString(urnStr);
    } catch (URISyntaxException e) {
      throw new RuntimeException(String.format("Failed to retrieve data product with urn %s, invalid urn", urnStr));
    }
  }

  @Override
  public SearchResults search(@Nonnull String query, @Nullable List<FacetFilterInput> filters,
      int start, int count, @Nonnull QueryContext context) throws Exception {
    final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
    final SearchResult searchResult = _entityClient.search(ENTITY_NAME, query, facetFilters, start,
        count, context.getAuthentication(), new SearchFlags().setFulltext(true));
    return UrnSearchResultsMapper.map(searchResult);

  }

  @Override
  public AutoCompleteResults autoComplete(@Nonnull String query, @Nullable String field,
      @Nullable Filter filters, int limit, @Nonnull QueryContext context) throws Exception {
    final AutoCompleteResult result = _entityClient.autoComplete(ENTITY_NAME, query, filters, limit, context.getAuthentication());
    return AutoCompleteResultsMapper.map(result);
  }

  @Override
  public Join update(String urn, @Nonnull JoinUpdateInput input, @Nonnull QueryContext context)
      throws Exception {
    if (isAuthorized(urn, input, context)) {
      final CorpuserUrn actor = CorpuserUrn.createFromString(context.getAuthentication().getActor().toUrnStr());

      // Same routine used by create - hence this check
      JoinUrn inputUrn = new JoinUrn(UUID.randomUUID().toString());
      if (urn != null) {
        inputUrn = JoinUrn.createFromString(urn);
        if ("new".equals(inputUrn.getJoinIdEntity())) {
          inputUrn = new JoinUrn(UUID.randomUUID().toString());
        }
      } else {
        urn = inputUrn.toString();
      }

      final JoinUrn updatedUrn = inputUrn;

      final Collection<MetadataChangeProposal> proposals = JoinUpdateInputMapper.map(input, actor);
      proposals.forEach(proposal -> proposal.setEntityUrn(updatedUrn));

      try {
        _entityClient.batchIngestProposals(proposals, context.getAuthentication(), false);
      } catch (RemoteInvocationException e) {
        throw new RuntimeException(String.format("Failed to write entity with urn %s", urn), e);
      }

      return load(urn, context).getData();
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  private boolean isAuthorized(String urn, JoinUpdateInput input, QueryContext context) {
    return true;
  }
}