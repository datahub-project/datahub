package com.linkedin.datahub.graphql.types.dataflow;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
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
import com.linkedin.datahub.graphql.types.dataflow.mappers.DataFlowMapper;
import com.linkedin.datahub.graphql.types.dataflow.mappers.DataFlowUpdateInputMapper;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowsePathsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowseResultMetadataMapper;
import com.linkedin.datahub.graphql.types.mappers.SearchResultsMapper;
import com.linkedin.datajob.client.DataFlows;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.common.CollectionResponse;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.datahub.graphql.Constants.BROWSE_PATH_DELIMITER;


public class DataFlowType implements SearchableEntityType<DataFlow>, BrowsableEntityType<DataFlow>, MutableType<DataFlowUpdateInput> {

    private static final Set<String> FACET_FIELDS = ImmutableSet.of("orchestrator", "cluster");
    private static final String DEFAULT_AUTO_COMPLETE_FIELD = "flowId";
    private final DataFlows _dataFlowsClient;

    public DataFlowType(final DataFlows dataFlowsClient) {
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
    public List<DataFlow> batchLoad(final List<String> urns, final QueryContext context) throws Exception {
        final List<DataFlowUrn> dataFlowUrns = urns.stream()
            .map(this::getDataFlowUrn)
            .collect(Collectors.toList());

        try {
            final Map<DataFlowUrn, com.linkedin.datajob.DataFlow> dataFlowMap = _dataFlowsClient.batchGet(dataFlowUrns
                .stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

            final List<com.linkedin.datajob.DataFlow> gmsResults = dataFlowUrns.stream()
                .map(flowUrn -> dataFlowMap.getOrDefault(flowUrn, null)).collect(Collectors.toList());

            return gmsResults.stream()
                .map(gmsDataFlow -> gmsDataFlow == null ? null : DataFlowMapper.map(gmsDataFlow))
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
        final CollectionResponse<com.linkedin.datajob.DataFlow> searchResult = _dataFlowsClient.search(query, facetFilters, start, count);
        return SearchResultsMapper.map(searchResult, DataFlowMapper::map);
    }

    @Override
    public AutoCompleteResults autoComplete(@Nonnull String query,
                                            @Nullable String field,
                                            @Nullable List<FacetFilterInput> filters,
                                            int limit,
                                            @Nonnull final QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        field = field != null ? field : DEFAULT_AUTO_COMPLETE_FIELD;
        final AutoCompleteResult result = _dataFlowsClient.autoComplete(query, field, facetFilters, limit);
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
                pathStr,
                facetFilters,
                start,
                count);
        final List<String> urns = result.getEntities().stream().map(entity -> entity.getUrn().toString()).collect(Collectors.toList());
        final List<DataFlow> dataFlows = batchLoad(urns, context);
        final BrowseResults browseResults = new BrowseResults();
        browseResults.setStart(result.getFrom());
        browseResults.setCount(result.getPageSize());
        browseResults.setTotal(result.getNumEntities());
        browseResults.setMetadata(BrowseResultMetadataMapper.map(result.getMetadata()));
        browseResults.setEntities(dataFlows.stream()
                .map(dataset -> (com.linkedin.datahub.graphql.generated.Entity) dataset)
                .collect(Collectors.toList()));
        return browseResults;
    }

    @Override
    public List<BrowsePath> browsePaths(@Nonnull String urn, @Nonnull QueryContext context) throws Exception {
        final StringArray result = _dataFlowsClient.getBrowsePaths(DataFlowUrn.createFromString(urn));
        return BrowsePathsMapper.map(result);
    }

    @Override
    public DataFlow update(@Nonnull DataFlowUpdateInput input, @Nonnull QueryContext context) throws Exception {

        final CorpuserUrn actor = CorpuserUrn.createFromString(context.getActor());
        final com.linkedin.datajob.DataFlow partialDataFlow = DataFlowUpdateInputMapper.map(input, actor);

        try {
            _dataFlowsClient.update(DataFlowUrn.createFromString(input.getUrn()), partialDataFlow);
        } catch (RemoteInvocationException e) {
            throw new RuntimeException(String.format("Failed to write entity with urn %s", input.getUrn()), e);
        }

        return load(input.getUrn(), context);
    }
}
