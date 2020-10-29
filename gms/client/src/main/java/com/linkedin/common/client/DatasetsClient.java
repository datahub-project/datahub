package com.linkedin.common.client;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataset.DatasetKey;
import com.linkedin.restli.client.Client;
import javax.annotation.Nonnull;

public abstract class DatasetsClient extends BaseClient {

    protected DatasetsClient(@Nonnull Client restliClient) {
        super(restliClient);
    }

    @Nonnull
    protected DatasetKey toDatasetKey(@Nonnull DatasetUrn urn) {
        return new DatasetKey()
                .setName(urn.getDatasetNameEntity())
                .setOrigin(urn.getOriginEntity())
                .setPlatform(urn.getPlatformEntity());
    }
}
