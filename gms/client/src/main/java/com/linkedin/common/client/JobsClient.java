package com.linkedin.common.client;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.JobUrn;
import com.linkedin.dataset.DatasetKey;
import com.linkedin.job.JobKey;
import com.linkedin.restli.client.Client;

import javax.annotation.Nonnull;

public class JobsClient  extends BaseClient {
    protected JobsClient(@Nonnull Client restliClient) {
        super(restliClient);
    }

    @Nonnull
    protected JobKey toJobKey(@Nonnull JobUrn urn) {
        return new JobKey()
                .setName(urn.getJobNameEntity())
                .setOrigin(urn.getOriginEntity())
                .setPlatform(urn.getPlatformEntity());
    }

    @Nonnull
    protected JobUrn toJobUrn(@Nonnull JobKey key) {
        return new JobUrn(key.getPlatform(), key.getName(), key.getOrigin());
    }
}
