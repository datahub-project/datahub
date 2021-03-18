package com.linkedin.datajob.client;

import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.BaseSearchableClient;
import com.linkedin.datajob.DataJob;
import com.linkedin.datajob.DataJobKey;
import com.linkedin.datajob.DataJobsDoAutocompleteRequestBuilder;
import com.linkedin.datajob.DataJobsFindBySearchRequestBuilder;
import com.linkedin.datajob.DataJobsRequestBuilders;
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

public class DataJobs extends BaseSearchableClient<DataJob> {
    private static final DataJobsRequestBuilders DATA_JOBS_REQUEST_BUILDERS = new DataJobsRequestBuilders();

    public DataJobs(@Nonnull Client restliClient) {
        super(restliClient);
    }

    @Nonnull
    @Override
    public CollectionResponse<DataJob> search(@Nonnull String input, @Nullable StringArray aspectNames,
        @Nullable Map<String, String> requestFilters, @Nullable SortCriterion sortCriterion, int start, int count)
        throws RemoteInvocationException {
        final DataJobsFindBySearchRequestBuilder requestBuilder = DATA_JOBS_REQUEST_BUILDERS.findBySearch()
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
    public CollectionResponse<DataJob> search(@Nonnull String input, int start, int count)
        throws RemoteInvocationException {
        return search(input, null, null, start, count);
    }

    /**
     * Gets {@link DataJob} model for the given urn
     *
     * @param urn data job urn
     * @return {@link DataJob} data job
     * @throws RemoteInvocationException
     */
    @Nonnull
    public DataJob get(@Nonnull DataJobUrn urn)
        throws RemoteInvocationException {
        GetRequest<DataJob> getRequest = DATA_JOBS_REQUEST_BUILDERS.get()
            .id(new ComplexResourceKey<>(toDataJobKey(urn), new EmptyRecord()))
            .build();

        return _client.sendRequest(getRequest).getResponse().getEntity();
    }

    /**
     * Searches for data jobs matching to a given query and filters
     *
     * @param input search query
     * @param requestFilters search filters
     * @param start start offset for search results
     * @param count max number of search results requested
     * @return CollectionResponse of {@link DataJob}
     * @throws RemoteInvocationException
     */
    @Nonnull
    public CollectionResponse<DataJob> search(@Nonnull String input, @Nonnull Map<String, String> requestFilters,
        int start, int count) throws RemoteInvocationException {

        DataJobsFindBySearchRequestBuilder requestBuilder = DATA_JOBS_REQUEST_BUILDERS
            .findBySearch()
            .inputParam(input)
            .filterParam(newFilter(requestFilters)).paginate(start, count);
        return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
    }

    /**
     * Autocomplete search for data jobs in search bar
     *
     * @param query search query
     * @param field field of the Data Job
     * @param requestFilters autocomplete filters
     * @param limit max number of autocomplete results
     * @throws RemoteInvocationException
     */
    @Nonnull
    public AutoCompleteResult autoComplete(@Nonnull String query, @Nonnull String field,
        @Nonnull Map<String, String> requestFilters,
        @Nonnull int limit) throws RemoteInvocationException {
        DataJobsDoAutocompleteRequestBuilder requestBuilder = DATA_JOBS_REQUEST_BUILDERS
            .actionAutocomplete()
            .queryParam(query)
            .fieldParam(field)
            .filterParam(newFilter(requestFilters))
            .limitParam(limit);
        return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
    }

    /**
     * Batch gets list of {@link DataJob}
     *
     * @param urns list of flow urn
     * @return map of {@link DataJob}
     * @throws RemoteInvocationException
     */
    @Nonnull
    public Map<DataJobUrn, DataJob> batchGet(@Nonnull Set<DataJobUrn> urns)
        throws RemoteInvocationException {
        BatchGetEntityRequest<ComplexResourceKey<DataJobKey, EmptyRecord>, DataJob> batchGetRequest
            = DATA_JOBS_REQUEST_BUILDERS.batchGet()
            .ids(urns.stream().map(this::getKeyFromUrn).collect(Collectors.toSet()))
            .build();

        return _client.sendRequest(batchGetRequest).getResponseEntity().getResults()
            .entrySet().stream().collect(Collectors.toMap(
                entry -> getUrnFromKey(entry.getKey()),
                entry -> entry.getValue().getEntity())
            );
    }

    @Nonnull
    private ComplexResourceKey<DataJobKey, EmptyRecord> getKeyFromUrn(@Nonnull DataJobUrn urn) {
        return new ComplexResourceKey<>(toDataJobKey(urn), new EmptyRecord());
    }

    @Nonnull
    private DataJobUrn getUrnFromKey(@Nonnull ComplexResourceKey<DataJobKey, EmptyRecord> key) {
        return toFlowUrn(key.getKey());
    }

    @Nonnull
    protected DataJobKey toDataJobKey(@Nonnull DataJobUrn urn) {
        return new DataJobKey()
            .setDataFlow(urn.getFlowEntity())
            .setJobId(urn.getJobIdEntity());
    }

    @Nonnull
    protected DataJobUrn toFlowUrn(@Nonnull DataJobKey key) {
        return new DataJobUrn(key.getDataFlow(), key.getJobId());
    }
}
