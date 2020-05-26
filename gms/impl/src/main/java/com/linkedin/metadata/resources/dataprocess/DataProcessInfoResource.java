package com.linkedin.metadata.resources.dataprocess;

import com.linkedin.dataprocess.DataProcessInfo;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;

import javax.annotation.Nonnull;

@RestLiCollection(name = "dataProcessInfo", namespace = "com.linkedin.dataprocess", parent = DataProcesses.class)
public class DataProcessInfoResource extends BaseDataProcessesAspectResource<DataProcessInfo> {

    public DataProcessInfoResource() {
        super(DataProcessInfo.class);
    }

    @Nonnull
    @Override
    @RestMethod.Create
    public Task<CreateResponse> create(@Nonnull DataProcessInfo dataProcessInfo) {
        return super.create(dataProcessInfo);
    }

    @Nonnull
    @Override
    @RestMethod.Get
    public Task<DataProcessInfo> get(@QueryParam("version") @Optional("0") @Nonnull Long version) {
        return super.get(version);
    }
}
