package com.linkedin.datahub.graphql.types.dataflow.mappers;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.datahub.graphql.generated.DataFlowUpdateInput;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipUpdateMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagAssociationUpdateMapper;
import com.linkedin.datajob.DataFlow;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class DataFlowUpdateInputMapper implements ModelMapper<DataFlowUpdateInput, DataFlow> {
    public static final DataFlowUpdateInputMapper INSTANCE = new DataFlowUpdateInputMapper();

    public static DataFlow map(@Nonnull final DataFlowUpdateInput dataFlowUpdateInput) {
        return INSTANCE.apply(dataFlowUpdateInput);
    }

    @Override
    public DataFlow apply(@Nonnull final DataFlowUpdateInput dataFlowUpdateInput) {
        final DataFlow result = new DataFlow();

        if (dataFlowUpdateInput.getOwnership() != null) {
            result.setOwnership(OwnershipUpdateMapper.map(dataFlowUpdateInput.getOwnership()));
        }

        if (dataFlowUpdateInput.getGlobalTags() != null) {
            final GlobalTags globalTags = new GlobalTags();
            globalTags.setTags(
                    new TagAssociationArray(
                            dataFlowUpdateInput.getGlobalTags().getTags().stream().map(
                                    element -> TagAssociationUpdateMapper.map(element)
                            ).collect(Collectors.toList())
                    )
            );
            result.setGlobalTags(globalTags);
        }
        return result;
    }
}