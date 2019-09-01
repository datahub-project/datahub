package com.linkedin.common.client;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.identity.CorpUserKey;
import com.linkedin.restli.client.Client;

import javax.annotation.Nonnull;

public class CorpUsersClient extends BaseClient {

    protected CorpUsersClient(@Nonnull Client restliClient) {
        super(restliClient);
    }

    @Nonnull
    protected CorpUserKey toCorpUserKey(@Nonnull CorpuserUrn urn) {
        return new CorpUserKey().setName(urn.getUsernameEntity());
    }

    @Nonnull
    protected CorpuserUrn toCorpUserUrn(@Nonnull CorpUserKey key) {
        return new CorpuserUrn(key.getName());
    }
}
