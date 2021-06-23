package com.linkedin.usage;

import com.linkedin.common.EntityRelationships;

import com.linkedin.common.WindowDuration;
import com.linkedin.common.client.BaseClient;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.server.annotations.Optional;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class UsageClient extends BaseClient {

    public UsageClient(@Nonnull Client restliClient) {
        super(restliClient);
    }
    private static final UsageStatsRequestBuilders USAGE_STATS_REQUEST_BUILDERS =
            new UsageStatsRequestBuilders();

    /**
     * Gets a specific version of downstream {@link EntityRelationships} for the given dataset.
     */
    @Nonnull
    public UsageQueryResult getUsageStats(
        @Nonnull String resource,
        @Nonnull WindowDuration duration,
        @Nullable @Optional Long startTime,
        @Nullable @Optional Long endTime,
        @Nullable @Optional Integer maxBuckets
        ) throws RemoteInvocationException, URISyntaxException {

        final UsageStatsDoQueryRequestBuilder request = USAGE_STATS_REQUEST_BUILDERS.
           actionQuery()
            .resourceParam(resource)
            .durationParam(duration);

        if (startTime != null) {
          request.startTimeParam(startTime);
        }

        if (endTime != null) {
            request.endTimeParam(endTime);
        }

        if (maxBuckets != null) {
            request.maxBucketsParam(maxBuckets);
        }

        return _client.sendRequest(request.build()).getResponseEntity();
    }
}
