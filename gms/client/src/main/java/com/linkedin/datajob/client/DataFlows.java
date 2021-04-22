package com.linkedin.datajob.client;

import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataflow.DataFlowsDoAutocompleteRequestBuilder;
import com.linkedin.dataflow.DataFlowsDoBrowseRequestBuilder;
import com.linkedin.dataflow.DataFlowsDoGetBrowsePathsRequestBuilder;
import com.linkedin.dataflow.DataFlowsFindBySearchRequestBuilder;
import com.linkedin.dataflow.DataFlowsRequestBuilders;
import com.linkedin.datajob.DataFlow;
import com.linkedin.datajob.DataFlowKey;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.BaseBrowsableClient;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.BatchGetEntityRequest;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.common.CollectionResponse;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.metadata.dao.utils.QueryUtils.newFilter;


public class DataFlows extends BaseBrowsableClient<DataFlow, DataFlowUrn> {
    private static final DataFlowsRequestBuilders DATA_FLOWS_REQUEST_BUILDERS = new DataFlowsRequestBuilders();

    public DataFlows(@Nonnull Client restliClient) {
        super(restliClient);
    }

    @Nonnull
    @Override
    public CollectionResponse<DataFlow> search(@Nonnull String input, @Nullable StringArray aspectNames,
        @Nullable Map<String, String> requestFilters, @Nullable SortCriterion sortCriterion, int start, int count)
        throws RemoteInvocationException {
        final DataFlowsFindBySearchRequestBuilder requestBuilder = DATA_FLOWS_REQUEST_BUILDERS.findBySearch()
            .aspectsParam(aspectNames)
            .inputParam(input)
            .sortParam(sortCriterion)
            .paginate(start, count);
        if (requestFilters != null) {
            requestBuilder.filterParam(newFilter(requestFilters));
        }
        return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
    }

    @Nonnull
    public CollectionResponse<DataFlow> search(@Nonnull String input, int start, int count)
        throws RemoteInvocationException {
        return search(input, null, null, start, count);
    }

    /**
     * Gets {@link DataFlow} model for the given urn
     *
     * @param urn data flow urn
     * @return {@link DataFlow} Data flow
     * @throws RemoteInvocationException
     */
    @Nonnull
    public DataFlow get(@Nonnull DataFlowUrn urn)
        throws RemoteInvocationException {
        GetRequest<DataFlow> getRequest = DATA_FLOWS_REQUEST_BUILDERS.get()
            .id(new ComplexResourceKey<>(toDataFlowKey(urn), new EmptyRecord()))
            .build();

        return _client.sendRequest(getRequest).getResponse().getEntity();
    }

    /**
     * Gets browse path(s) given dataset urn
     *
     * @param urn urn for the entity
     * @return list of paths given urn
     * @throws RemoteInvocationException
     */
    @Nonnull
    @Override
    public StringArray getBrowsePaths(@Nonnull DataFlowUrn urn) throws RemoteInvocationException {
        DataFlowsDoGetBrowsePathsRequestBuilder requestBuilder = DATA_FLOWS_REQUEST_BUILDERS
            .actionGetBrowsePaths()
            .urnParam(urn);
        return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
    }

    /**
     * Gets browse snapshot of a given path
     *
     * @param path path being browsed
     * @param requestFilters browse filters
     * @param start start offset of first dataset
     * @param limit max number of datasets
     * @throws RemoteInvocationException
     */
    @Nonnull
    @Override
    public BrowseResult browse(@Nonnull String path, @Nullable Map<String, String> requestFilters,
        int start, int limit) throws RemoteInvocationException {
        DataFlowsDoBrowseRequestBuilder requestBuilder = DATA_FLOWS_REQUEST_BUILDERS
            .actionBrowse()
            .pathParam(path)
            .startParam(start)
            .limitParam(limit);
        if (requestFilters != null) {
            requestBuilder.filterParam(newFilter(requestFilters));
        }
        return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
    }

    /**
     * Searches for data flows matching to a given query and filters
     *
     * @param input search query
     * @param requestFilters search filters
     * @param start start offset for search results
     * @param count max number of search results requested
     * @return CollectionResponse of {@link DataFlow}
     * @throws RemoteInvocationException
     */
    @Nonnull
    public CollectionResponse<DataFlow> search(@Nonnull String input, @Nonnull Map<String, String> requestFilters,
        int start, int count) throws RemoteInvocationException {

        DataFlowsFindBySearchRequestBuilder requestBuilder = DATA_FLOWS_REQUEST_BUILDERS
            .findBySearch()
            .inputParam(input)
            .filterParam(newFilter(requestFilters)).paginate(start, count);
        return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
    }

    /**
     * Autocomplete search for data flows in search bar
     *
     * @param query search query
     * @param field field of the Data Flow
     * @param requestFilters autocomplete filters
     * @param limit max number of autocomplete results
     * @throws RemoteInvocationException
     */
    @Nonnull
    public AutoCompleteResult autoComplete(@Nonnull String query, @Nonnull String field,
        @Nonnull Map<String, String> requestFilters,
        @Nonnull int limit) throws RemoteInvocationException {
        DataFlowsDoAutocompleteRequestBuilder requestBuilder = DATA_FLOWS_REQUEST_BUILDERS
            .actionAutocomplete()
            .queryParam(query)
            .fieldParam(field)
            .filterParam(newFilter(requestFilters))
            .limitParam(limit);
        return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
    }

    /**
     * Batch gets list of {@link DataFlow}
     *
     * @param urns list of flow urn
     * @return map of {@link DataFlow}
     * @throws RemoteInvocationException
     */
    @Nonnull
    public Map<DataFlowUrn, DataFlow> batchGet(@Nonnull Set<DataFlowUrn> urns)
        throws RemoteInvocationException {
        BatchGetEntityRequest<ComplexResourceKey<DataFlowKey, EmptyRecord>, DataFlow> batchGetRequest
            = DATA_FLOWS_REQUEST_BUILDERS.batchGet()
            .ids(urns.stream().map(this::getKeyFromUrn).collect(Collectors.toSet()))
            .build();

        return _client.sendRequest(batchGetRequest).getResponseEntity().getResults()
            .entrySet().stream().collect(Collectors.toMap(
                entry -> getUrnFromKey(entry.getKey()),
                entry -> entry.getValue().getEntity())
            );
    }

    @Nonnull
    private ComplexResourceKey<DataFlowKey, EmptyRecord> getKeyFromUrn(@Nonnull DataFlowUrn urn) {
        return new ComplexResourceKey<>(toDataFlowKey(urn), new EmptyRecord());
    }

    @Nonnull
    private DataFlowUrn getUrnFromKey(@Nonnull ComplexResourceKey<DataFlowKey, EmptyRecord> key) {
        return toFlowUrn(key.getKey());
    }

    @Nonnull
    protected DataFlowKey toDataFlowKey(@Nonnull DataFlowUrn urn) {
        return new DataFlowKey()
            .setOrchestrator(urn.getOrchestratorEntity())
            .setFlowId(urn.getFlowIdEntity())
            .setCluster(urn.getClusterEntity());
    }

    @Nonnull
    protected DataFlowUrn toFlowUrn(@Nonnull DataFlowKey key) {
        return new DataFlowUrn(key.getOrchestrator(), key.getFlowId(), key.getCluster());
    }
}
