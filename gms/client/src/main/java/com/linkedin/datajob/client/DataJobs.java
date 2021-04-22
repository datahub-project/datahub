package com.linkedin.datajob.client;

import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datajob.DataJobsDoBrowseRequestBuilder;
import com.linkedin.datajob.DataJobsDoGetBrowsePathsRequestBuilder;
import com.linkedin.metadata.aspect.DataJobAspect;
import com.linkedin.metadata.dao.DataJobActionRequestBuilder;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.BaseBrowsableClient;
import com.linkedin.datajob.DataJob;
import com.linkedin.datajob.DataJobKey;
import com.linkedin.datajob.DataJobsDoAutocompleteRequestBuilder;
import com.linkedin.datajob.DataJobsFindBySearchRequestBuilder;
import com.linkedin.datajob.DataJobsRequestBuilders;
import com.linkedin.metadata.snapshot.DataJobSnapshot;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.BatchGetEntityRequest;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.common.CollectionResponse;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.metadata.dao.utils.QueryUtils.*;

public class DataJobs extends BaseBrowsableClient<DataJob, DataJobUrn> {
    private static final DataJobsRequestBuilders DATA_JOBS_REQUEST_BUILDERS = new DataJobsRequestBuilders();
    private static final DataJobActionRequestBuilder DATA_JOB_ACTION_REQUEST_BUILDER = new DataJobActionRequestBuilder();

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
     * Gets browse path(s) given dataset urn
     *
     * @param urn urn for the entity
     * @return list of paths given urn
     * @throws RemoteInvocationException
     */
    @Nonnull
    @Override
    public StringArray getBrowsePaths(@Nonnull DataJobUrn urn) throws RemoteInvocationException {
        DataJobsDoGetBrowsePathsRequestBuilder requestBuilder = DATA_JOBS_REQUEST_BUILDERS
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
        DataJobsDoBrowseRequestBuilder requestBuilder = DATA_JOBS_REQUEST_BUILDERS
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

    /**
     * Update an existing DataJob
     */
    public void update(@Nonnull final DataJobUrn urn, @Nonnull final DataJob dataJob) throws RemoteInvocationException {
        Request request = DATA_JOB_ACTION_REQUEST_BUILDER.createRequest(urn, toSnapshot(dataJob, urn));
        _client.sendRequest(request).getResponse();
    }

    static DataJobSnapshot toSnapshot(@Nonnull DataJob dataJob, @Nonnull DataJobUrn urn) {
        final List<DataJobAspect> aspects = new ArrayList<>();
        if (dataJob.hasInfo()) {
            aspects.add(ModelUtils.newAspectUnion(DataJobAspect.class, dataJob.getInfo()));
        }

        if (dataJob.hasOwnership()) {
            aspects.add(ModelUtils.newAspectUnion(DataJobAspect.class, dataJob.getOwnership()));
        }
        if (dataJob.hasStatus()) {
            aspects.add(ModelUtils.newAspectUnion(DataJobAspect.class, dataJob.getStatus()));
        }
        if (dataJob.hasGlobalTags()) {
            aspects.add(ModelUtils.newAspectUnion(DataJobAspect.class, dataJob.getGlobalTags()));
        }
        return ModelUtils.newSnapshot(DataJobSnapshot.class, urn, aspects);
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
