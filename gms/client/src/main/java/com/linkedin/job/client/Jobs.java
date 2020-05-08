package com.linkedin.job.client;

import com.linkedin.common.urn.JobUrn;
import com.linkedin.job.*;
import com.linkedin.metadata.configs.JobSearchConfig;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.BaseClient;
import com.linkedin.metadata.restli.SearchableClient;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.*;
import com.linkedin.restli.common.CollectionResponse;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.linkedin.metadata.dao.utils.QueryUtils.newFilter;

public class Jobs extends BaseClient implements SearchableClient<Job> {
    private static final JobsRequestBuilders JOBS_REQUEST_BUILDERS = new JobsRequestBuilders();
    private static final JobInfoRequestBuilders JOB_INFO_REQUEST_BUILDERS = new JobInfoRequestBuilders();
    private static final JobSearchConfig JOB_SEARCH_CONFIG = new JobSearchConfig();

    public Jobs(@Nonnull Client restliClient) {
        super(restliClient);
    }

    @Nonnull
    public Job get(@Nonnull JobUrn urn)
            throws RemoteInvocationException {
        GetRequest<Job> getRequest = JOBS_REQUEST_BUILDERS.get()
                .id(new ComplexResourceKey<>(toJobKey(urn), new EmptyRecord()))
                .build();

        return _client.sendRequest(getRequest).getResponse().getEntity();
    }

    @Nonnull
    public Map<JobUrn, Job> batchGet(@Nonnull Set<JobUrn> urns)
            throws RemoteInvocationException {
        BatchGetEntityRequest<ComplexResourceKey<JobKey, EmptyRecord>, Job> batchGetRequest
                = JOBS_REQUEST_BUILDERS.batchGet()
                .ids(urns.stream().map(this::getKeyFromUrn).collect(Collectors.toSet()))
                .build();

        return _client.sendRequest(batchGetRequest).getResponseEntity().getResults()
                .entrySet().stream().collect(Collectors.toMap(
                        entry -> getUrnFromKey(entry.getKey()),
                        entry -> entry.getValue().getEntity())
                );
    }

//    @Nonnull
//    public List<Job> getAll(int start, int count)
//            throws RemoteInvocationException {
//        final GetAllRequest<Job> getAllRequest = JOBS_REQUEST_BUILDERS.batchGet()
//                .paginate(start, count)
//                .build();
//        return _client.sendRequest(getAllRequest).getResponseEntity().getElements();
//    }
//
//    public void createJobInfo(@Nonnull JobUrn jobUrn,
//                                   @Nonnull JobInfo jobInfo) throws RemoteInvocationException {
//        CreateIdRequest<Long, JobInfo> request = JOB_INFO_REQUEST_BUILDERS.create()
//                .jobKey(new ComplexResourceKey<>(toJobKey(jobUrn), new EmptyRecord()))
//                .input(jobInfo)
//                .build();
//        _client.sendRequest(request).getResponse();
//    }

    @Nonnull
    public CollectionResponse<Job> search(@Nonnull String input, @Nullable Map<String, String> requestFilters,
                                               @Nullable SortCriterion sortCriterion, int start, int count) throws RemoteInvocationException {
        final Filter filter = (requestFilters != null) ? newFilter(requestFilters) : null;
        JobsFindBySearchRequestBuilder requestBuilder = JOBS_REQUEST_BUILDERS
                .findBySearch()
                .inputParam(input)
                .filterParam(filter)
                .sortParam(sortCriterion)
                .paginate(start, count);
        return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
    }

    @Nonnull
    public CollectionResponse<Job> search(@Nonnull String input, @Nullable Map<String, String> requestFilters,
                                               int start, int count) throws RemoteInvocationException {
        return search(input, requestFilters, null, start, count);
    }

    @Nonnull
    public CollectionResponse<Job> search(@Nonnull String input, int start, int count)
            throws RemoteInvocationException {
        return search(input, null, null, start, count);
    }

    @Nonnull
    public AutoCompleteResult autocomplete(@Nonnull String query, @Nullable String field,
                                           @Nullable Map<String, String> requestFilters, int limit) throws RemoteInvocationException {
        final String autocompleteField = (field != null) ? field : JOB_SEARCH_CONFIG.getDefaultAutocompleteField();
        final Filter filter = (requestFilters != null) ? newFilter(requestFilters) : null;
        JobsDoAutocompleteRequestBuilder requestBuilder = JOBS_REQUEST_BUILDERS
                .actionAutocomplete()
                .queryParam(query)
                .fieldParam(autocompleteField)
                .filterParam(filter)
                .limitParam(limit);
        return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
    }

    @Nonnull
    private JobKey toJobKey(@Nonnull JobUrn urn) {
        return new JobKey().setName(urn.getJobNameEntity());
    }

    @Nonnull
    protected JobUrn toJobUrn(@Nonnull JobKey key) {
        return new JobUrn(key.getPlatform(), key.getName(), key.getOrigin());
    }

    @Nonnull
    private ComplexResourceKey<JobKey, EmptyRecord> getKeyFromUrn(@Nonnull JobUrn urn) {
        return new ComplexResourceKey<>(toJobKey(urn), new EmptyRecord());
    }

    @Nonnull
    private JobUrn getUrnFromKey(@Nonnull ComplexResourceKey<JobKey, EmptyRecord> key) {
        return toJobUrn(key.getKey());
    }
}
