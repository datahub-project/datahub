package com.linkedin.datahub.graphql.types.dataflow;

import com.datahub.metadata.authorization.AuthorizationRequest;
import com.datahub.metadata.authorization.AuthorizationResult;
import com.datahub.metadata.authorization.Authorizer;
import com.google.common.collect.ImmutableSet;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.PoliciesConfig;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.DataFlow;
import com.linkedin.datahub.graphql.generated.DataFlowUpdateInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.MutableType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.dataflow.mappers.DataFlowSnapshotMapper;
import com.linkedin.datahub.graphql.types.dataflow.mappers.DataFlowUpdateInputSnapshotMapper;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowsePathsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowseResultMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.extractor.AspectExtractor;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SearchResult;
import com.linkedin.metadata.snapshot.DataFlowSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.datahub.graphql.Constants.BROWSE_PATH_DELIMITER;


public class DataFlowType implements SearchableEntityType<DataFlow>, BrowsableEntityType<DataFlow>, MutableType<DataFlowUpdateInput> {

    private static final Set<String> FACET_FIELDS = ImmutableSet.of("orchestrator", "cluster");
    private final EntityClient _dataFlowsClient;

    public DataFlowType(final EntityClient dataFlowsClient) {
        _dataFlowsClient = dataFlowsClient;
    }

    @Override
    public EntityType type() {
        return EntityType.DATA_FLOW;
    }

    @Override
    public Class<DataFlow> objectClass() {
        return DataFlow.class;
    }

    @Override
    public Class<DataFlowUpdateInput> inputClass() {
        return DataFlowUpdateInput.class;
    }

    @Override
    public List<DataFetcherResult<DataFlow>> batchLoad(final List<String> urns, final QueryContext context) throws Exception {
        final List<DataFlowUrn> dataFlowUrns = urns.stream()
            .map(this::getDataFlowUrn)
            .collect(Collectors.toList());

        try {
            final Map<Urn, Entity> dataFlowMap = _dataFlowsClient.batchGet(dataFlowUrns
                .stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()),
            context.getActor());

            final List<Entity> gmsResults = dataFlowUrns.stream()
                .map(flowUrn -> dataFlowMap.getOrDefault(flowUrn, null)).collect(Collectors.toList());

            return gmsResults.stream()
                .map(gmsDataFlow -> gmsDataFlow == null ? null : DataFetcherResult.<DataFlow>newResult()
                    .data(DataFlowSnapshotMapper.map(gmsDataFlow.getValue().getDataFlowSnapshot()))
                    .localContext(AspectExtractor.extractAspects(gmsDataFlow.getValue().getDataFlowSnapshot()))
                    .build())
                .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load DataFlows", e);
        }
    }

    @Override
    public SearchResults search(@Nonnull String query,
                                @Nullable List<FacetFilterInput> filters,
                                int start,
                                int count,
                                @Nonnull final QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final SearchResult searchResult = _dataFlowsClient.search("dataFlow", query, facetFilters, start, count, context.getActor());
        return UrnSearchResultsMapper.map(searchResult);
    }

    @Override
    public AutoCompleteResults autoComplete(@Nonnull String query,
                                            @Nullable String field,
                                            @Nullable List<FacetFilterInput> filters,
                                            int limit,
                                            @Nonnull final QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final AutoCompleteResult result = _dataFlowsClient.autoComplete("dataFlow", query, facetFilters, limit, context.getActor());
        return AutoCompleteResultsMapper.map(result);
    }

    private DataFlowUrn getDataFlowUrn(String urnStr) {
        try {
            return DataFlowUrn.createFromString(urnStr);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Failed to retrieve dataflow with urn %s, invalid urn", urnStr));
        }
    }

    @Override
    public BrowseResults browse(@Nonnull List<String> path, @Nullable List<FacetFilterInput> filters, int start,
        int count, @Nonnull QueryContext context) throws Exception {
                final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final String pathStr = path.size() > 0 ? BROWSE_PATH_DELIMITER + String.join(BROWSE_PATH_DELIMITER, path) : "";
        final BrowseResult result = _dataFlowsClient.browse(
            "dataFlow",
                pathStr,
                facetFilters,
                start,
                count,
            context.getActor());
        return BrowseResultMapper.map(result);
    }

    @Override
    public List<BrowsePath> browsePaths(@Nonnull String urn, @Nonnull QueryContext context) throws Exception {
        final StringArray result = _dataFlowsClient.getBrowsePaths(DataFlowUrn.createFromString(urn), context.getActor());
        return BrowsePathsMapper.map(result);
    }

    @Override
    public DataFlow update(@Nonnull DataFlowUpdateInput input, @Nonnull QueryContext context) throws Exception {

        if (isAuthorized(input, context)) {
            final CorpuserUrn actor = CorpuserUrn.createFromString(context.getActor());
            final DataFlowSnapshot dataFlowSnapshot = DataFlowUpdateInputSnapshotMapper.map(input, actor);
            final Snapshot snapshot = Snapshot.create(dataFlowSnapshot);

            try {
                Entity entity = new Entity();
                entity.setValue(snapshot);
                _dataFlowsClient.update(entity, context.getActor());
            } catch (RemoteInvocationException e) {
                throw new RuntimeException(String.format("Failed to write entity with urn %s", input.getUrn()), e);
            }

            return load(input.getUrn(), context).getData();
        }
        throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    private boolean isAuthorized(@Nonnull DataFlowUpdateInput update, @Nonnull QueryContext context) {
        // Decide whether the current principal should be allowed to update the Dataset.
        // First, check what is being updated.
        final Authorizer authorizer = context.getAuthorizer();
        final String principal = context.getActor();
        final String resourceUrn = update.getUrn();
        final String resourceType = PoliciesConfig.DATA_FLOW_PRIVILEGES.getResourceType();
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
        for (final String privilege : privilegeGroup) {
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

    private List<List<String>> getRequiredPrivileges(final DataFlowUpdateInput updateInput) {
        List<List<String>> orPrivileges = new ArrayList<>();

        List<String> allEntityPrivileges = new ArrayList<>();
        allEntityPrivileges.add(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType());

        List<String> andPrivileges = new ArrayList<>();
        if (updateInput.getOwnership() != null) {
            andPrivileges.add(PoliciesConfig.EDIT_ENTITY_OWNERS_PRIVILEGE.getType());
        }
        if (updateInput.getEditableProperties() != null) {
            andPrivileges.add(PoliciesConfig.EDIT_ENTITY_DOCS_PRIVILEGE.getType());
        }
        if (updateInput.getGlobalTags() != null) {
            andPrivileges.add(PoliciesConfig.EDIT_ENTITY_TAGS_PRIVILEGE.getType());
        }

        // If either set of privileges are all true, permit the operation.
        orPrivileges.add(allEntityPrivileges);
        orPrivileges.add(andPrivileges);
        return orPrivileges;
    }
}
