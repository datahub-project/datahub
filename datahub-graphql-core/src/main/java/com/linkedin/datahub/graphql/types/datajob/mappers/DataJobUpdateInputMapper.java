package com.linkedin.datahub.graphql.types.datajob.mappers;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.datahub.graphql.generated.DataJobUpdateInput;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipUpdateMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagAssociationUpdateMapper;
import com.linkedin.datajob.DataJob;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class DataJobUpdateInputMapper implements ModelMapper<DataJobUpdateInput, DataJob> {
    public static final DataJobUpdateInputMapper INSTANCE = new DataJobUpdateInputMapper();

    public static DataJob map(@Nonnull final DataJobUpdateInput dataJobUpdateInput) {
        return INSTANCE.apply(dataJobUpdateInput);
    }

    @Override
    public DataJob apply(@Nonnull final DataJobUpdateInput dataJobUpdateInput) {
        final DataJob result = new DataJob();

        if (dataJobUpdateInput.getOwnership() != null) {
            result.setOwnership(OwnershipUpdateMapper.map(dataJobUpdateInput.getOwnership()));
        }

        if (dataJobUpdateInput.getGlobalTags() != null) {
            final GlobalTags globalTags = new GlobalTags();
            globalTags.setTags(
                    new TagAssociationArray(
                            dataJobUpdateInput.getGlobalTags().getTags().stream().map(
                                    element -> TagAssociationUpdateMapper.map(element)
                            ).collect(Collectors.toList())
                    )
            );
            result.setGlobalTags(globalTags);
        }
        return result;
    }
}