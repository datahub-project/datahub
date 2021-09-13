package com.linkedin.metadata.dao;

import com.linkedin.common.urn.MLModelUrn;
import com.linkedin.metadata.snapshot.MLModelSnapshot;

public class MLModelActionRequestBuilder extends BaseActionRequestBuilder<MLModelSnapshot, MLModelUrn> {

    private static final String BASE_URI_TEMPLATE = "mlModels";

    public MLModelActionRequestBuilder() {
        super(MLModelSnapshot.class, MLModelUrn.class, BASE_URI_TEMPLATE);
    }
}