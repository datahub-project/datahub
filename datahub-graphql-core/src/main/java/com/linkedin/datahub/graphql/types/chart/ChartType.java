package com.linkedin.datahub.graphql.types.chart;

import static com.linkedin.datahub.graphql.Constants.*;
import static com.linkedin.metadata.Constants.*;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.ChartUrn;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.datahub.graphql.generated.ChartUpdateInput;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.MutableType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.chart.mappers.ChartMapper;
import com.linkedin.datahub.graphql.types.chart.mappers.ChartUpdateInputMapper;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowsePathsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowseResultMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.query.AutoCompleteResult;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ChartType
    implements SearchableEntityType<Chart, String>,
        BrowsableEntityType<Chart, String>,
        MutableType<ChartUpdateInput, Chart> {

  private static final Set<String> ASPECTS_TO_RESOLVE =
      ImmutableSet.of(
          CHART_KEY_ASPECT_NAME,
          CHART_INFO_ASPECT_NAME,
          EDITABLE_CHART_PROPERTIES_ASPECT_NAME,
          CHART_QUERY_ASPECT_NAME,
          OWNERSHIP_ASPECT_NAME,
          INSTITUTIONAL_MEMORY_ASPECT_NAME,
          GLOBAL_TAGS_ASPECT_NAME,
          GLOSSARY_TERMS_ASPECT_NAME,
          STATUS_ASPECT_NAME,
          CONTAINER_ASPECT_NAME,
          DOMAINS_ASPECT_NAME,
          DEPRECATION_ASPECT_NAME,
          DATA_PLATFORM_INSTANCE_ASPECT_NAME,
          INPUT_FIELDS_ASPECT_NAME,
          EMBED_ASPECT_NAME,
          DATA_PRODUCTS_ASPECT_NAME,
          BROWSE_PATHS_V2_ASPECT_NAME,
          SUB_TYPES_ASPECT_NAME,
          STRUCTURED_PROPERTIES_ASPECT_NAME,
          FORMS_ASPECT_NAME,
          APPLICATION_MEMBERSHIP_ASPECT_NAME);
  private static final Set<String> FACET_FIELDS =
      ImmutableSet.of("access", "queryType", "tool", "type");

  private final EntityClient _entityClient;

  public ChartType(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public Class<ChartUpdateInput> inputClass() {
    return ChartUpdateInput.class;
  }

  @Override
  public EntityType type() {
    return EntityType.CHART;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<Chart> objectClass() {
    return Chart.class;
  }

  @Override
  public List<DataFetcherResult<Chart>> batchLoad(
      @Nonnull List<String> urnStrs, @Nonnull QueryContext context) throws Exception {
    final List<Urn> urns = urnStrs.stream().map(UrnUtils::getUrn).collect(Collectors.toList());
    try {

      final Map<Urn, EntityResponse> chartMap =
          _entityClient.batchGetV2(
              context.getOperationContext(),
              CHART_ENTITY_NAME,
              new HashSet<>(urns),
              ASPECTS_TO_RESOLVE);

      final List<EntityResponse> gmsResults = new ArrayList<>(urnStrs.size());
      for (Urn urn : urns) {
        gmsResults.add(chartMap.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(
              gmsChart ->
                  gmsChart == null
                      ? null
                      : DataFetcherResult.<Chart>newResult()
                          .data(ChartMapper.map(context, gmsChart))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Charts", e);
    }
  }

  @Override
  public SearchResults search(
      @Nonnull String query,
      @Nullable List<FacetFilterInput> filters,
      int start,
      @Nullable Integer count,
      @Nonnull QueryContext context)
      throws Exception {
    final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
    final SearchResult searchResult =
        _entityClient.search(
            context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
            "chart",
            query,
            facetFilters,
            start,
            count);
    return UrnSearchResultsMapper.map(context, searchResult);
  }

  @Override
  public AutoCompleteResults autoComplete(
      @Nonnull String query,
      @Nullable String field,
      @Nullable Filter filters,
      @Nullable Integer limit,
      @Nonnull QueryContext context)
      throws Exception {
    final AutoCompleteResult result =
        _entityClient.autoComplete(context.getOperationContext(), "chart", query, filters, limit);
    return AutoCompleteResultsMapper.map(context, result);
  }

  @Override
  public BrowseResults browse(
      @Nonnull List<String> path,
      @Nullable List<FacetFilterInput> filters,
      int start,
      @Nullable Integer count,
      @Nonnull QueryContext context)
      throws Exception {
    final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
    final String pathStr =
        path.size() > 0 ? BROWSE_PATH_DELIMITER + String.join(BROWSE_PATH_DELIMITER, path) : "";
    final BrowseResult result =
        _entityClient.browse(
            context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(false)),
            "chart",
            pathStr,
            facetFilters,
            start,
            count);
    return BrowseResultMapper.map(context, result);
  }

  @Override
  public List<BrowsePath> browsePaths(@Nonnull String urn, @Nonnull QueryContext context)
      throws Exception {
    final StringArray result =
        _entityClient.getBrowsePaths(context.getOperationContext(), getChartUrn(urn));
    return BrowsePathsMapper.map(context, result);
  }

  private ChartUrn getChartUrn(String urnStr) {
    try {
      return ChartUrn.createFromString(urnStr);
    } catch (URISyntaxException e) {
      throw new RuntimeException(
          String.format("Failed to retrieve chart with urn %s, invalid urn", urnStr));
    }
  }

  @Override
  public Chart update(
      @Nonnull String urn, @Nonnull ChartUpdateInput input, @Nonnull QueryContext context)
      throws Exception {
    if (isAuthorized(urn, input, context)) {
      final CorpuserUrn actor = CorpuserUrn.createFromString(context.getActorUrn());
      final Collection<MetadataChangeProposal> proposals =
          ChartUpdateInputMapper.map(context, input, actor);
      proposals.forEach(proposal -> proposal.setEntityUrn(UrnUtils.getUrn(urn)));

      try {
        _entityClient.batchIngestProposals(context.getOperationContext(), proposals, false);
      } catch (RemoteInvocationException e) {
        throw new RuntimeException(String.format("Failed to write entity with urn %s", urn), e);
      }

      return load(urn, context).getData();
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  private boolean isAuthorized(
      @Nonnull String urn, @Nonnull ChartUpdateInput update, @Nonnull QueryContext context) {
    // Decide whether the current principal should be allowed to update the Dataset.
    final DisjunctivePrivilegeGroup orPrivilegeGroups = getAuthorizedPrivileges(update);
    return AuthorizationUtils.isAuthorized(
        context, PoliciesConfig.CHART_PRIVILEGES.getResourceType(), urn, orPrivilegeGroups);
  }

  private DisjunctivePrivilegeGroup getAuthorizedPrivileges(final ChartUpdateInput updateInput) {

    final ConjunctivePrivilegeGroup allPrivilegesGroup =
        new ConjunctivePrivilegeGroup(
            ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()));

    List<String> specificPrivileges = new ArrayList<>();
    if (updateInput.getOwnership() != null) {
      specificPrivileges.add(PoliciesConfig.EDIT_ENTITY_OWNERS_PRIVILEGE.getType());
    }
    if (updateInput.getEditableProperties() != null) {
      specificPrivileges.add(PoliciesConfig.EDIT_ENTITY_DOCS_PRIVILEGE.getType());
    }
    if (updateInput.getGlobalTags() != null) {
      specificPrivileges.add(PoliciesConfig.EDIT_ENTITY_TAGS_PRIVILEGE.getType());
    }
    final ConjunctivePrivilegeGroup specificPrivilegeGroup =
        new ConjunctivePrivilegeGroup(specificPrivileges);

    // If you either have all entity privileges, or have the specific privileges required, you are
    // authorized.
    return new DisjunctivePrivilegeGroup(
        ImmutableList.of(allPrivilegesGroup, specificPrivilegeGroup));
  }
}
