package com.linkedin.datahub.graphql.types.dataset;

import static com.linkedin.datahub.graphql.Constants.*;
import static com.linkedin.metadata.Constants.*;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.BatchDatasetUpdateInput;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.DatasetUpdateInput;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.BatchMutableType;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.dataset.mappers.DatasetMapper;
import com.linkedin.datahub.graphql.types.dataset.mappers.DatasetUpdateInputMapper;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowsePathsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowseResultMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DatasetType
    implements SearchableEntityType<Dataset, String>,
        BrowsableEntityType<Dataset, String>,
        BatchMutableType<DatasetUpdateInput, BatchDatasetUpdateInput, Dataset> {

  private static final Set<String> ASPECTS_TO_RESOLVE =
      ImmutableSet.of(
          DATASET_KEY_ASPECT_NAME,
          DATASET_PROPERTIES_ASPECT_NAME,
          EDITABLE_DATASET_PROPERTIES_ASPECT_NAME,
          DATASET_DEPRECATION_ASPECT_NAME, // This aspect is deprecated.
          DEPRECATION_ASPECT_NAME,
          DATASET_UPSTREAM_LINEAGE_ASPECT_NAME,
          UPSTREAM_LINEAGE_ASPECT_NAME,
          EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
          VIEW_PROPERTIES_ASPECT_NAME,
          OWNERSHIP_ASPECT_NAME,
          INSTITUTIONAL_MEMORY_ASPECT_NAME,
          GLOBAL_TAGS_ASPECT_NAME,
          GLOSSARY_TERMS_ASPECT_NAME,
          STATUS_ASPECT_NAME,
          CONTAINER_ASPECT_NAME,
          DOMAINS_ASPECT_NAME,
          SCHEMA_METADATA_ASPECT_NAME,
          DATA_PLATFORM_INSTANCE_ASPECT_NAME,
          SIBLINGS_ASPECT_NAME,
          EMBED_ASPECT_NAME,
          DATA_PRODUCTS_ASPECT_NAME,
          BROWSE_PATHS_V2_ASPECT_NAME,
          ACCESS_DATASET_ASPECT_NAME,
          SUB_TYPES_ASPECT_NAME);

  private static final Set<String> FACET_FIELDS = ImmutableSet.of("origin", "platform");
  private static final String ENTITY_NAME = "dataset";

  private final EntityClient _entityClient;

  public DatasetType(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public Class<Dataset> objectClass() {
    return Dataset.class;
  }

  @Override
  public Class<DatasetUpdateInput> inputClass() {
    return DatasetUpdateInput.class;
  }

  @Override
  public Class<BatchDatasetUpdateInput[]> batchInputClass() {
    return BatchDatasetUpdateInput[].class;
  }

  @Override
  public EntityType type() {
    return EntityType.DATASET;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public List<DataFetcherResult<Dataset>> batchLoad(
      @Nonnull final List<String> urnStrs, @Nonnull final QueryContext context) {
    try {
      final List<Urn> urns = urnStrs.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

      final Map<Urn, EntityResponse> datasetMap =
          _entityClient.batchGetV2(
              Constants.DATASET_ENTITY_NAME,
              new HashSet<>(urns),
              ASPECTS_TO_RESOLVE,
              context.getAuthentication());

      final List<EntityResponse> gmsResults = new ArrayList<>();
      for (Urn urn : urns) {
        gmsResults.add(datasetMap.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(
              gmsDataset ->
                  gmsDataset == null
                      ? null
                      : DataFetcherResult.<Dataset>newResult()
                          .data(DatasetMapper.map(gmsDataset))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Datasets", e);
    }
  }

  @Override
  public SearchResults search(
      @Nonnull String query,
      @Nullable List<FacetFilterInput> filters,
      int start,
      int count,
      @Nonnull final QueryContext context)
      throws Exception {
    final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
    final SearchResult searchResult =
        _entityClient.search(
            ENTITY_NAME,
            query,
            facetFilters,
            start,
            count,
            context.getAuthentication(),
            new SearchFlags().setFulltext(true));
    return UrnSearchResultsMapper.map(searchResult);
  }

  @Override
  public AutoCompleteResults autoComplete(
      @Nonnull String query,
      @Nullable String field,
      @Nullable Filter filters,
      int limit,
      @Nonnull final QueryContext context)
      throws Exception {
    final AutoCompleteResult result =
        _entityClient.autoComplete(ENTITY_NAME, query, filters, limit, context.getAuthentication());
    return AutoCompleteResultsMapper.map(result);
  }

  @Override
  public BrowseResults browse(
      @Nonnull List<String> path,
      @Nullable List<FacetFilterInput> filters,
      int start,
      int count,
      @Nonnull final QueryContext context)
      throws Exception {
    final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
    final String pathStr =
        path.size() > 0 ? BROWSE_PATH_DELIMITER + String.join(BROWSE_PATH_DELIMITER, path) : "";
    final BrowseResult result =
        _entityClient.browse(
            "dataset", pathStr, facetFilters, start, count, context.getAuthentication());
    return BrowseResultMapper.map(result);
  }

  @Override
  public List<BrowsePath> browsePaths(@Nonnull String urn, @Nonnull final QueryContext context)
      throws Exception {
    final StringArray result =
        _entityClient.getBrowsePaths(DatasetUtils.getDatasetUrn(urn), context.getAuthentication());
    return BrowsePathsMapper.map(result);
  }

  @Override
  public List<Dataset> batchUpdate(
      @Nonnull BatchDatasetUpdateInput[] input, @Nonnull QueryContext context) throws Exception {
    final Urn actor = Urn.createFromString(context.getAuthentication().getActor().toUrnStr());

    final Collection<MetadataChangeProposal> proposals =
        Arrays.stream(input)
            .map(
                updateInput -> {
                  if (isAuthorized(updateInput.getUrn(), updateInput.getUpdate(), context)) {
                    Collection<MetadataChangeProposal> datasetProposals =
                        DatasetUpdateInputMapper.map(updateInput.getUpdate(), actor);
                    datasetProposals.forEach(
                        proposal -> proposal.setEntityUrn(UrnUtils.getUrn(updateInput.getUrn())));
                    return datasetProposals;
                  }
                  throw new AuthorizationException(
                      "Unauthorized to perform this action. Please contact your DataHub administrator.");
                })
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

    final List<String> urns =
        Arrays.stream(input).map(BatchDatasetUpdateInput::getUrn).collect(Collectors.toList());

    try {
      _entityClient.batchIngestProposals(proposals, context.getAuthentication(), false);
    } catch (RemoteInvocationException e) {
      throw new RuntimeException(String.format("Failed to write entity with urn %s", urns), e);
    }

    return batchLoad(urns, context).stream()
        .map(DataFetcherResult::getData)
        .collect(Collectors.toList());
  }

  @Override
  public Dataset update(
      @Nonnull String urn, @Nonnull DatasetUpdateInput input, @Nonnull QueryContext context)
      throws Exception {
    if (isAuthorized(urn, input, context)) {
      final CorpuserUrn actor =
          CorpuserUrn.createFromString(context.getAuthentication().getActor().toUrnStr());
      final Collection<MetadataChangeProposal> proposals =
          DatasetUpdateInputMapper.map(input, actor);
      proposals.forEach(proposal -> proposal.setEntityUrn(UrnUtils.getUrn(urn)));

      try {
        _entityClient.batchIngestProposals(proposals, context.getAuthentication(), false);
      } catch (RemoteInvocationException e) {
        throw new RuntimeException(String.format("Failed to write entity with urn %s", urn), e);
      }

      return load(urn, context).getData();
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  private boolean isAuthorized(
      @Nonnull String urn, @Nonnull DatasetUpdateInput update, @Nonnull QueryContext context) {
    // Decide whether the current principal should be allowed to update the Dataset.
    final DisjunctivePrivilegeGroup orPrivilegeGroups = getAuthorizedPrivileges(update);
    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getAuthentication().getActor().toUrnStr(),
        PoliciesConfig.DATASET_PRIVILEGES.getResourceType(),
        urn,
        orPrivilegeGroups);
  }

  private DisjunctivePrivilegeGroup getAuthorizedPrivileges(final DatasetUpdateInput updateInput) {

    final ConjunctivePrivilegeGroup allPrivilegesGroup =
        new ConjunctivePrivilegeGroup(
            ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()));

    List<String> specificPrivileges = new ArrayList<>();
    if (updateInput.getInstitutionalMemory() != null) {
      specificPrivileges.add(PoliciesConfig.EDIT_ENTITY_DOC_LINKS_PRIVILEGE.getType());
    }
    if (updateInput.getOwnership() != null) {
      specificPrivileges.add(PoliciesConfig.EDIT_ENTITY_OWNERS_PRIVILEGE.getType());
    }
    if (updateInput.getDeprecation() != null) {
      specificPrivileges.add(PoliciesConfig.EDIT_ENTITY_STATUS_PRIVILEGE.getType());
    }
    if (updateInput.getEditableProperties() != null) {
      specificPrivileges.add(PoliciesConfig.EDIT_ENTITY_DOCS_PRIVILEGE.getType());
    }
    if (updateInput.getGlobalTags() != null) {
      specificPrivileges.add(PoliciesConfig.EDIT_ENTITY_TAGS_PRIVILEGE.getType());
    }
    if (updateInput.getEditableSchemaMetadata() != null) {
      specificPrivileges.add(PoliciesConfig.EDIT_DATASET_COL_TAGS_PRIVILEGE.getType());
      specificPrivileges.add(PoliciesConfig.EDIT_DATASET_COL_DESCRIPTION_PRIVILEGE.getType());
    }

    final ConjunctivePrivilegeGroup specificPrivilegeGroup =
        new ConjunctivePrivilegeGroup(specificPrivileges);

    // If you either have all entity privileges, or have the specific privileges required, you are
    // authorized.
    return new DisjunctivePrivilegeGroup(
        ImmutableList.of(allPrivilegesGroup, specificPrivilegeGroup));
  }
}
