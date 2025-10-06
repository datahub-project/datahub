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
import com.linkedin.datahub.graphql.AspectMappingRegistry;
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
          ACCESS_ASPECT_NAME,
          STRUCTURED_PROPERTIES_ASPECT_NAME,
          FORMS_ASPECT_NAME,
          SUB_TYPES_ASPECT_NAME,
          APPLICATION_MEMBERSHIP_ASPECT_NAME,
          VERSION_PROPERTIES_ASPECT_NAME,
          LOGICAL_PARENT_ASPECT_NAME,
          ASSET_SETTINGS_ASPECT_NAME);

  private static final Set<String> FACET_FIELDS = ImmutableSet.of("origin", "platform");
  private static final String ENTITY_NAME = "dataset";

  private final EntityClient entityClient;

  public DatasetType(final EntityClient entityClient) {
    this.entityClient = entityClient;
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
      
      // Access DataFetchingEnvironment from QueryContext
      if (context.getDataFetchingEnvironment() != null) {
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~DataFetchingEnvironment Available~~~~~~~~~~~~~~~");
        System.out.println("Requested fields: " + context.getDataFetchingEnvironment().getSelectionSet().getFields().keySet());
        System.out.println("Field name: " + context.getDataFetchingEnvironment().getField().getName());
        System.out.println("Arguments: " + context.getDataFetchingEnvironment().getArguments());
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        
        // You can now optimize which aspects to fetch based on requested fields
        Set<String> aspectsToResolve = determineAspectsFromRequestedFields(
            context.getDataFetchingEnvironment().getSelectionSet().getFields().keySet()
        );
        System.out.println("Optimized aspects to resolve: " + aspectsToResolve);
      } else {
        System.out.println("DataFetchingEnvironment not available, using default aspects");
      }

      final Map<Urn, EntityResponse> datasetMap =
          entityClient.batchGetV2(
              context.getOperationContext(),
              Constants.DATASET_ENTITY_NAME,
              new HashSet<>(urns),
              ASPECTS_TO_RESOLVE);

      final List<EntityResponse> gmsResults = new ArrayList<>(urnStrs.size());
      for (Urn urn : urns) {
        gmsResults.add(datasetMap.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(
              gmsDataset ->
                  gmsDataset == null
                      ? null
                      : DataFetcherResult.<Dataset>newResult()
                          .data(DatasetMapper.map(context, gmsDataset))
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
      @Nullable Integer count,
      @Nonnull final QueryContext context)
      throws Exception {
    final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
    final SearchResult searchResult =
        entityClient.search(
            context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
            ENTITY_NAME,
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
      @Nonnull final QueryContext context)
      throws Exception {
    final AutoCompleteResult result =
        entityClient.autoComplete(
            context.getOperationContext(), ENTITY_NAME, query, filters, limit);
    return AutoCompleteResultsMapper.map(context, result);
  }

  @Override
  public BrowseResults browse(
      @Nonnull List<String> path,
      @Nullable List<FacetFilterInput> filters,
      int start,
      @Nullable Integer count,
      @Nonnull final QueryContext context)
      throws Exception {
    final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
    final String pathStr =
        path.size() > 0 ? BROWSE_PATH_DELIMITER + String.join(BROWSE_PATH_DELIMITER, path) : "";
    final BrowseResult result =
        entityClient.browse(
            context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(false)),
            "dataset",
            pathStr,
            facetFilters,
            start,
            count);
    return BrowseResultMapper.map(context, result);
  }

  @Override
  public List<BrowsePath> browsePaths(@Nonnull String urn, @Nonnull final QueryContext context)
      throws Exception {
    final StringArray result =
        entityClient.getBrowsePaths(context.getOperationContext(), DatasetUtils.getDatasetUrn(urn));
    return BrowsePathsMapper.map(context, result);
  }

  @Override
  public List<Dataset> batchUpdate(
      @Nonnull BatchDatasetUpdateInput[] input, @Nonnull QueryContext context) throws Exception {
    final Urn actor = Urn.createFromString(context.getActorUrn());

    final Collection<MetadataChangeProposal> proposals =
        Arrays.stream(input)
            .map(
                updateInput -> {
                  if (isAuthorized(updateInput.getUrn(), updateInput.getUpdate(), context)) {
                    Collection<MetadataChangeProposal> datasetProposals =
                        DatasetUpdateInputMapper.map(context, updateInput.getUpdate(), actor);
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
      entityClient.batchIngestProposals(context.getOperationContext(), proposals, false);
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
      final CorpuserUrn actor = CorpuserUrn.createFromString(context.getActorUrn());
      final Collection<MetadataChangeProposal> proposals =
          DatasetUpdateInputMapper.map(context, input, actor);
      proposals.forEach(proposal -> proposal.setEntityUrn(UrnUtils.getUrn(urn)));

      try {
        entityClient.batchIngestProposals(context.getOperationContext(), proposals, false);
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
        context, PoliciesConfig.DATASET_PRIVILEGES.getResourceType(), urn, orPrivilegeGroups);
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

  /**
   * Maps GraphQL requested fields to the corresponding aspects that need to be fetched.
   * This enables performance optimization by only fetching the aspects needed for the requested fields.
   *
   * @param requestedFields Set of field names requested in the GraphQL query
   * @return Set of aspect names that should be fetched
   */
  private Set<String> determineAspectsFromRequestedFields(Set<String> requestedFields) {
    Set<String> aspectsToResolve = new HashSet<>();

    // Always include basic aspects
    aspectsToResolve.add(DATASET_KEY_ASPECT_NAME);
    aspectsToResolve.add(STATUS_ASPECT_NAME);

    // Map specific fields to their corresponding aspects
    for (String field : requestedFields) {
      switch (field) {
        case "properties":
          aspectsToResolve.add(DATASET_PROPERTIES_ASPECT_NAME);
          break;
        case "editableProperties":
          aspectsToResolve.add(EDITABLE_DATASET_PROPERTIES_ASPECT_NAME);
          break;
        case "deprecation":
          aspectsToResolve.add(DATASET_DEPRECATION_ASPECT_NAME);
          aspectsToResolve.add(DEPRECATION_ASPECT_NAME);
          break;
        case "upstream":
        case "lineage":
          aspectsToResolve.add(DATASET_UPSTREAM_LINEAGE_ASPECT_NAME);
          aspectsToResolve.add(UPSTREAM_LINEAGE_ASPECT_NAME);
          break;
        case "schemaMetadata":
          aspectsToResolve.add(SCHEMA_METADATA_ASPECT_NAME);
          break;
        case "editableSchemaMetadata":
          aspectsToResolve.add(EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
          break;
        case "viewProperties":
          aspectsToResolve.add(VIEW_PROPERTIES_ASPECT_NAME);
          break;
        case "ownership":
          aspectsToResolve.add(OWNERSHIP_ASPECT_NAME);
          break;
        case "institutionalMemory":
          aspectsToResolve.add(INSTITUTIONAL_MEMORY_ASPECT_NAME);
          break;
        case "tags":
          aspectsToResolve.add(GLOBAL_TAGS_ASPECT_NAME);
          break;
        case "glossaryTerms":
          aspectsToResolve.add(GLOSSARY_TERMS_ASPECT_NAME);
          break;
        case "container":
          aspectsToResolve.add(CONTAINER_ASPECT_NAME);
          break;
        case "domain":
          aspectsToResolve.add(DOMAINS_ASPECT_NAME);
          break;
        case "dataPlatformInstance":
          aspectsToResolve.add(DATA_PLATFORM_INSTANCE_ASPECT_NAME);
          break;
        case "siblings":
          aspectsToResolve.add(SIBLINGS_ASPECT_NAME);
          break;
        case "embed":
          aspectsToResolve.add(EMBED_ASPECT_NAME);
          break;
        case "dataProducts":
          aspectsToResolve.add(DATA_PRODUCTS_ASPECT_NAME);
          break;
        case "browsePaths":
          aspectsToResolve.add(BROWSE_PATHS_V2_ASPECT_NAME);
          break;
        case "access":
          aspectsToResolve.add(ACCESS_ASPECT_NAME);
          break;
        case "structuredProperties":
          aspectsToResolve.add(STRUCTURED_PROPERTIES_ASPECT_NAME);
          break;
        case "forms":
          aspectsToResolve.add(FORMS_ASPECT_NAME);
          break;
        case "subTypes":
          aspectsToResolve.add(SUB_TYPES_ASPECT_NAME);
          break;
        case "application":
          aspectsToResolve.add(APPLICATION_MEMBERSHIP_ASPECT_NAME);
          break;
        case "versionProperties":
          aspectsToResolve.add(VERSION_PROPERTIES_ASPECT_NAME);
          break;
        case "logicalParent":
          aspectsToResolve.add(LOGICAL_PARENT_ASPECT_NAME);
          break;
        case "share":
          aspectsToResolve.add(SHARE_ASPECT_NAME);
          break;
        case "origin":
          aspectsToResolve.add(ORIGIN_ASPECT_NAME);
          break;
        case "documentation":
          aspectsToResolve.add(DOCUMENTATION_ASPECT_NAME);
          break;
        case "lineageFeatures":
          aspectsToResolve.add(LINEAGE_FEATURES_ASPECT_NAME);
          break;
        default:
          // For unknown fields, don't add any specific aspects
          break;
      }
    }

    // If no specific aspects were determined, fall back to all aspects
    if (aspectsToResolve.size() <= 2) { // Only basic aspects were added
      return ASPECTS_TO_RESOLVE;
    }

    return aspectsToResolve;
  }
}
