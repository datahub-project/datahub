package com.linkedin.common.client;

import com.linkedin.common.urn.MLModelUrn;
import com.linkedin.ml.MLModelKey;
import com.linkedin.restli.client.Client;
import javax.annotation.Nonnull;

public abstract class MLModelsClient extends BaseClient {

    protected MLModelsClient(@Nonnull Client restliClient) {
        super(restliClient);
    }

    @Nonnull
    protected MLModelKey toMLModelKey(@Nonnull MLModelUrn urn) {
        return new MLModelKey()
            .setName(urn.getMlModelNameEntity())
            .setOrigin(urn.getOriginEntity())
            .setPlatform(urn.getPlatformEntity());
    }

    @Nonnull
    protected MLModelUrn toModelUrn(@Nonnull MLModelKey key) {
        return new MLModelUrn(key.getPlatform(), key.getName(), key.getOrigin());
    }
}
