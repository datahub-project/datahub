package com.linkedin.datahub.graphql.types.dataset;

import com.datahub.metadata.authorization.AuthorizationRequest;
import com.datahub.metadata.authorization.AuthorizationResult;
import com.datahub.metadata.authorization.Authorizer;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.PolicyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.DatasetUpdateInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.MutableType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.types.dataset.mappers.DatasetUpdateInputSnapshotMapper;
import com.linkedin.datahub.graphql.types.dataset.mappers.DatasetSnapshotMapper;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowsePathsMapper;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.mappers.BrowseResultMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.extractor.AspectExtractor;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SearchResult;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.r2.RemoteInvocationException;

import graphql.execution.DataFetcherResult;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.linkedin.datahub.graphql.Constants.BROWSE_PATH_DELIMITER;

public class DatasetType implements SearchableEntityType<Dataset>, BrowsableEntityType<Dataset>, MutableType<DatasetUpdateInput> {

    private static final Set<String> FACET_FIELDS = ImmutableSet.of("origin", "platform");
    private static final String ENTITY_NAME = "dataset";

    private final EntityClient _datasetsClient;

    public DatasetType(final EntityClient datasetsClient) {
        _datasetsClient = datasetsClient;
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
    public EntityType type() {
        return EntityType.DATASET;
    }

    @Override
    public List<DataFetcherResult<Dataset>> batchLoad(final List<String> urns, final QueryContext context) {

        final List<DatasetUrn> datasetUrns = urns.stream()
                .map(DatasetUtils::getDatasetUrn)
                .collect(Collectors.toList());

        try {
            final Map<Urn, Entity> datasetMap = _datasetsClient.batchGet(datasetUrns
                    .stream()
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet()),
                context.getActor());

            final List<Entity> gmsResults = new ArrayList<>();
            for (DatasetUrn urn : datasetUrns) {
                gmsResults.add(datasetMap.getOrDefault(urn, null));
            }
            return gmsResults.stream()
                .map(gmsDataset ->
                    gmsDataset == null ? null : DataFetcherResult.<Dataset>newResult()
                        .data(DatasetSnapshotMapper.map(gmsDataset.getValue().getDatasetSnapshot()))
                        .localContext(AspectExtractor.extractAspects(gmsDataset.getValue().getDatasetSnapshot()))
                        .build()
                )
                .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load Datasets", e);
        }
    }

    @Override
    public SearchResults search(@Nonnull String query,
                                @Nullable List<FacetFilterInput> filters,
                                int start,
                                int count,
                                @Nonnull final QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final SearchResult searchResult = _datasetsClient.search(ENTITY_NAME, query, facetFilters, start, count, context.getActor());
        return UrnSearchResultsMapper.map(searchResult);
    }

    @Override
    public AutoCompleteResults autoComplete(@Nonnull String query,
                                            @Nullable String field,
                                            @Nullable List<FacetFilterInput> filters,
                                            int limit,
                                            @Nonnull final QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final AutoCompleteResult result = _datasetsClient.autoComplete(ENTITY_NAME, query, facetFilters, limit, context.getActor());
        return AutoCompleteResultsMapper.map(result);
    }

    @Override
    public BrowseResults browse(@Nonnull List<String> path,
                                @Nullable List<FacetFilterInput> filters,
                                int start,
                                int count,
                                @Nonnull final QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final String pathStr = path.size() > 0 ? BROWSE_PATH_DELIMITER + String.join(BROWSE_PATH_DELIMITER, path) : "";
        final BrowseResult result = _datasetsClient.browse(
                "dataset",
                pathStr,
                facetFilters,
                start,
                count,
            context.getActor());
        return BrowseResultMapper.map(result);
    }

    @Override
    public List<BrowsePath> browsePaths(@Nonnull String urn, @Nonnull final QueryContext context) throws Exception {
        final StringArray result = _datasetsClient.getBrowsePaths(DatasetUtils.getDatasetUrn(urn), context.getActor());
        return BrowsePathsMapper.map(result);
    }

    @Override
    public Dataset update(@Nonnull DatasetUpdateInput input, @Nonnull QueryContext context) throws Exception {
        if (isAuthorized(input, context)) {
            final CorpuserUrn actor = CorpuserUrn.createFromString(context.getActor());
            final DatasetSnapshot datasetSnapshot = DatasetUpdateInputSnapshotMapper.map(input, actor);
            final Snapshot snapshot = Snapshot.create(datasetSnapshot);

            try {
                Entity entity = new Entity();
                entity.setValue(snapshot);
                _datasetsClient.update(entity, context.getActor());
            } catch (RemoteInvocationException e) {
                throw new RuntimeException(String.format("Failed to write entity with urn %s", input.getUrn()), e);
            }

            return load(input.getUrn(), context).getData();
        }
        throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    // TODO: Can we extract this into a common helper?
    private boolean isAuthorized(@Nonnull DatasetUpdateInput update, @Nonnull QueryContext context) {
        // Decide whether the current principal should be allowed to update the Dataset.
        // First, check what is being updated.
        final Authorizer authorizer = context.getAuthorizer();
        final String principal = context.getActor();
        final String resourceUrn = update.getUrn();
        final String resourceType = PolicyUtils.DATASET_ENTITY_RESOURCE_TYPE;
        final List<List<String>> requiredPrivileges = getRequiredPrivileges(update);
        final AuthorizationRequest.ResourceSpec resourceSpec = new AuthorizationRequest.ResourceSpec(resourceType, resourceUrn);

        for (List<String> privilegeGroup : requiredPrivileges) {
            if (isAuthorized(principal, privilegeGroup, resourceSpec, authorizer)) {
                return true;
            }
        }
        return false;
    }

    private boolean isAuthorized(
        String principal,
        List<String> privilegeGroup,
        AuthorizationRequest.ResourceSpec resourceSpec,
        Authorizer authorizer) {
        // Each privilege in a group _must_ all be true to permit the operation.
        for (String privilege : privilegeGroup) {
            // No "partial" operations. All privileges required for the update must be granted for it to succeed.
            final AuthorizationRequest request = new AuthorizationRequest(principal, privilege, Optional.of(resourceSpec));
            final AuthorizationResult result = authorizer.authorize(request);
            if (AuthorizationResult.Type.DENY.equals(result.getType())) {
                // Short circuit.
                return false;
            }
        }
        return true;
    }


    // Returns a disjunction of conjunctive sets of privileges. TODO: model this more legibly..
    private List<List<String>> getRequiredPrivileges(final DatasetUpdateInput updateInput) {
        List<List<String>> orPrivileges = new ArrayList<>();

        List<String> allEntityPrivileges = new ArrayList<>();
        allEntityPrivileges.add(PolicyUtils.EDIT_ENTITY);

        List<String> andPrivileges = new ArrayList<>();
        if (updateInput.getInstitutionalMemory() != null) {
            andPrivileges.add(PolicyUtils.EDIT_ENTITY_DOC_LINKS);
        }
        if (updateInput.getOwnership() != null) {
            andPrivileges.add(PolicyUtils.EDIT_ENTITY_OWNERS);
        }
        if (updateInput.getDeprecation() != null) {
            andPrivileges.add(PolicyUtils.EDIT_ENTITY_STATUS);
        }
        if (updateInput.getEditableProperties() != null) {
            andPrivileges.add(PolicyUtils.EDIT_ENTITY_DOCS);
        }
        if (updateInput.getGlobalTags() != null) {
            andPrivileges.add(PolicyUtils.EDIT_ENTITY_TAGS);
        }
        if (updateInput.getEditableSchemaMetadata() != null) {
            andPrivileges.add(PolicyUtils.EDIT_DATASET_COL_TAGS);
            andPrivileges.add(PolicyUtils.EDIT_DATASET_COL_DESCRIPTION);
        }

        // If either set of privileges are all true, permit the operation.
        orPrivileges.add(allEntityPrivileges);
        orPrivileges.add(andPrivileges);
        return orPrivileges;
    }
}
