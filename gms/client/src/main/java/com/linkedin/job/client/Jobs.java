package com.linkedin.job.client;

import com.linkedin.job.Job;
import com.linkedin.job.JobInfoRequestBuilders;
import com.linkedin.job.JobsRequestBuilders;
import com.linkedin.metadata.configs.JobSearchConfig;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.restli.BaseClient;
import com.linkedin.metadata.restli.SearchableClient;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.common.CollectionResponse;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

public class Jobs extends BaseClient implements SearchableClient<Job> {
    private static final JobsRequestBuilders JOBS_REQUEST_BUILDERS = new JobsRequestBuilders();
    private static final JobInfoRequestBuilders JOB_INFO_REQUEST_BUILDERS = new JobInfoRequestBuilders();
    private static final JobSearchConfig JOB_SEARCH_CONFIG = new JobSearchConfig();

    public Jobs(@Nonnull Client restliClient) {
        super(restliClient);
    }
    @Nonnull
    @Override
    public CollectionResponse<Job> search(@Nonnull String input, @Nullable Map<String, String> requestFilters, int start, int count) throws RemoteInvocationException {
        return null;
    }

    @Nonnull
    @Override
    public AutoCompleteResult autocomplete(@Nonnull String query, @Nullable String field, @Nullable Map<String, String> requestFilters, int limit) throws RemoteInvocationException {
        return null;
    }
}
