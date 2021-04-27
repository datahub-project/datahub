package com.linkedin.datahub.graphql.types.dataflow.mappers;

import com.linkedin.datahub.graphql.generated.DataFlow;
import com.linkedin.datahub.graphql.generated.DataFlowInfo;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;

import javax.annotation.Nonnull;

public class DataFlowMapper implements ModelMapper<com.linkedin.datajob.DataFlow, DataFlow> {

    public static final DataFlowMapper INSTANCE = new DataFlowMapper();

    public static DataFlow map(@Nonnull final com.linkedin.datajob.DataFlow dataflow) {
        return INSTANCE.apply(dataflow);
    }

    @Override
    public DataFlow apply(@Nonnull final com.linkedin.datajob.DataFlow dataflow) {
        final DataFlow result = new DataFlow();
        result.setUrn(dataflow.getUrn().toString());
        result.setType(EntityType.DATA_FLOW);
        result.setOrchestrator(dataflow.getOrchestrator());
        result.setFlowId(dataflow.getFlowId());
        result.setCluster(dataflow.getCluster());
        if (dataflow.hasInfo()) {
            result.setInfo(mapDataFlowInfo(dataflow.getInfo()));
        }
        if (dataflow.hasOwnership()) {
            result.setOwnership(OwnershipMapper.map(dataflow.getOwnership()));
        }
        if (dataflow.hasGlobalTags()) {
            result.setGlobalTags(GlobalTagsMapper.map(dataflow.getGlobalTags()));
        }
        return result;
    }

    private DataFlowInfo mapDataFlowInfo(final com.linkedin.datajob.DataFlowInfo info) {
        final DataFlowInfo result = new DataFlowInfo();
        result.setName(info.getName());
        result.setDescription(info.getDescription());
        result.setProject(info.getProject());
        if (info.hasExternalUrl()) {
            result.setExternalUrl(info.getExternalUrl().toString());
        }
        if (info.hasCustomProperties()) {
            result.setCustomProperties(StringMapMapper.map(info.getCustomProperties()));
        }
        return result;
    }
}
