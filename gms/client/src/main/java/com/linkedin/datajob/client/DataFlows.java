package com.linkedin.datajob.client;

import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.BaseSearchableClient;
import com.linkedin.datajob.DataFlow;
import com.linkedin.datajob.DataFlowKey;
import com.linkedin.datajob.DataFlowsDoAutocompleteRequestBuilder;
import com.linkedin.datajob.DataFlowsFindBySearchRequestBuilder;
import com.linkedin.datajob.DataFlowsRequestBuilders;
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

import static com.linkedin.metadata.dao.utils.QueryUtils.*;

public class DataFlows extends BaseSearchableClient<DataFlow> {
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
