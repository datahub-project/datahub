package com.linkedin.metadata.dao;

import com.linkedin.common.urn.DataProcessUrn;
import com.linkedin.metadata.snapshot.DataProcessSnapshot;

public class DataProcessActionRequestBuilder extends BaseActionRequestBuilder<DataProcessSnapshot, DataProcessUrn> {
    private static final String BASE_URI_TEMPLATE = "dataProcesses";

    public DataProcessActionRequestBuilder() {
        super(DataProcessSnapshot.class, DataProcessUrn.class, BASE_URI_TEMPLATE);
    }
}
