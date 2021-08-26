package com.linkedin.common.client;

import com.linkedin.common.urn.DataProcessUrn;
import com.linkedin.dataprocess.DataProcessResourceKey;
import com.linkedin.restli.client.Client;

import javax.annotation.Nonnull;

public class DataProcessesClient  extends BaseClient {
    protected DataProcessesClient(@Nonnull Client restliClient) {
        super(restliClient);
    }

    @Nonnull
    protected DataProcessResourceKey toDataProcessKey(@Nonnull DataProcessUrn urn) {
        return new DataProcessResourceKey()
                .setName(urn.getNameEntity())
                .setOrigin(urn.getOriginEntity())
                .setOrchestrator(urn.getOrchestratorEntity());
    }

    @Nonnull
    protected DataProcessUrn toDataProcessUrn(@Nonnull DataProcessResourceKey key) {
        return new DataProcessUrn(key.getOrchestrator(), key.getName(), key.getOrigin());
    }
}
