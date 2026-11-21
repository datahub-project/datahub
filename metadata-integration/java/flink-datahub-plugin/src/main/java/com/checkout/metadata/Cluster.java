package com.checkout.metadata;

import com.checkout.utils.ConfigUtils;
import com.linkedin.common.FabricType;
import com.linkedin.common.TimeStamp;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.metadata.key.DataFlowKey;
import datahub.event.MetadataChangeProposalWrapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;

import java.util.ArrayList;
import java.util.List;

public class Cluster {
    /*
        Flink Cluster maps to a DataFlow in DataHub. Here we extract all the information we want to push to DataHub.
    */

    private final StringMap properties;
    private String clusterId;
    public DataFlowUrn dataFlowUrn;
    private final long startTimeMilliseconds;

    public Cluster(Configuration configuration, long startTimeMilliseconds) {
        this.properties = ConfigUtils.getConfigMap(configuration);

        String jobName = configuration.getString(PipelineOptions.NAME);
        try {
            clusterId = ConfigUtils.getClusterId(configuration);
        } catch (IllegalStateException e) {
            clusterId = jobName;
        }
        this.dataFlowUrn = getUrn();
        this.startTimeMilliseconds = startTimeMilliseconds;
    }

    public DataFlowUrn getUrn() {
        return new DataFlowUrn("flink", clusterId, "kubernetes");
    }

    public List<MetadataChangeProposalWrapper<?>> getProposals() {
        List<MetadataChangeProposalWrapper<?>> proposals = new ArrayList<>();
        proposals.add(constructKey());
        proposals.add(constructInfo());
        return proposals;
    }

    private MetadataChangeProposalWrapper<?> constructKey() {
        return MetadataChangeProposalWrapper.builder()
                .entityType("dataFlow")
                .entityUrn(dataFlowUrn)
                .upsert()
                .aspect(new DataFlowKey()
                        .setOrchestrator("flink")
                        .setCluster("PROD")
                        .setFlowId(clusterId))
                .build();
    }

    private MetadataChangeProposalWrapper<?> constructInfo() {
        return MetadataChangeProposalWrapper.builder()
                .entityType("dataFlow")
                .entityUrn(dataFlowUrn)
                .upsert()
                .aspect(new DataFlowInfo()
                        .setName(clusterId)
                        .setCreated(new TimeStamp().setTime(startTimeMilliseconds))
                        .setCustomProperties(properties)
                        .setEnv(FabricType.PROD)
                ).build();
    }


}
