package com.checkout.metadata;

import com.checkout.utils.ConfigUtils;
import com.linkedin.common.FabricType;
import com.linkedin.common.TimeStamp;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.datajob.azkaban.AzkabanJobType;
import com.linkedin.metadata.key.DataJobKey;
import datahub.event.MetadataChangeProposalWrapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;

import java.util.ArrayList;

public class Job {

    private final Configuration configuration;
    private final long startTimeMilliseconds;
    private final String jobName;
    private final DataFlowUrn dataFlowUrn;
    private final DataJobUrn dataJobUrn;


    /*
    We map from a FlinkJob to a DataJob in DataHub.
     */
    public Job(Configuration configuration, long startTimeMilliseconds, DataFlowUrn dataFlowUrn) {
        this.configuration = configuration;
        this.startTimeMilliseconds = startTimeMilliseconds;
        this.dataFlowUrn = dataFlowUrn;

        this.jobName = configuration.getString(PipelineOptions.NAME);
        this.dataJobUrn = constructUrn();
    }

    public DataJobUrn constructUrn() {
        return new DataJobUrn(dataFlowUrn, jobName);
    }

    public DataJobUrn getUrn() {
        return dataJobUrn;
    }

    public ArrayList<MetadataChangeProposalWrapper<?>> getProposals() {
        ArrayList<MetadataChangeProposalWrapper<?>> proposals = new ArrayList<>();
        proposals.add(constructKey());
        proposals.add(constructInfo());
        return proposals;
    }

    private MetadataChangeProposalWrapper<?> constructKey() {
        return MetadataChangeProposalWrapper.builder()
                .entityType("dataJob")
                .entityUrn(dataJobUrn)
                .upsert()
                .aspect(new DataJobKey()
                        .setJobId(jobName)
                        .setFlow(dataFlowUrn)
                ).build();
    }

    private MetadataChangeProposalWrapper<?> constructInfo() {
        return MetadataChangeProposalWrapper.builder()
                .entityType("dataJob")
                .entityUrn(dataJobUrn)
                .upsert()
                .aspect(new DataJobInfo()
                        .setName(jobName)
                        .setCreated(new TimeStamp().setTime(startTimeMilliseconds))
                        .setDescription("")
                        .setEnv(FabricType.PROD)
                        .setFlowUrn(dataFlowUrn)
                        .setCustomProperties(ConfigUtils.getConfigMap(configuration))
                        .setType(DataJobInfo.Type.create(AzkabanJobType.SQL))
                        .setLastModified(new TimeStamp().setTime(startTimeMilliseconds))
                ).build();
    }


}
