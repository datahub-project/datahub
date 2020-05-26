package com.linkedin.common.client;

import com.linkedin.common.urn.DataProcessUrn;
import com.linkedin.dataprocess.DataProcessKey;
import com.linkedin.restli.client.Client;

import javax.annotation.Nonnull;

public class DataProcessesClient  extends BaseClient {
    protected DataProcessesClient(@Nonnull Client restliClient) {
        super(restliClient);
    }

    @Nonnull
    protected DataProcessKey toDataProcessKey(@Nonnull DataProcessUrn urn) {
        return new DataProcessKey()
                .setName(urn.getProcessNameEntity())
                .setOrigin(urn.getOriginEntity())
                .setOrchestrator(urn.getDataProcessOrchestrator());
    }

    @Nonnull
    protected DataProcessUrn toDataProcessUrn(@Nonnull DataProcessKey key) {
        return new DataProcessUrn(key.getOrchestrator(), key.getName(), key.getOrigin());
    }
}
