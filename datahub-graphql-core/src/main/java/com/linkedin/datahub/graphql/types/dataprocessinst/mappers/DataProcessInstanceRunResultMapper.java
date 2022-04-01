package com.linkedin.datahub.graphql.types.dataprocessinst.mappers;

import com.linkedin.datahub.graphql.generated.DataProcessInstanceRunResultType;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.dataprocess.DataProcessInstanceRunResult;
import javax.annotation.Nonnull;


public class DataProcessInstanceRunResultMapper implements ModelMapper<
    DataProcessInstanceRunResult, com.linkedin.datahub.graphql.generated.DataProcessInstanceRunResult> {

    public static final DataProcessInstanceRunResultMapper INSTANCE = new DataProcessInstanceRunResultMapper();

    public static com.linkedin.datahub.graphql.generated.DataProcessInstanceRunResult map(@Nonnull final DataProcessInstanceRunResult input) {
        return INSTANCE.apply(input);
    }

    @Override
    public com.linkedin.datahub.graphql.generated.DataProcessInstanceRunResult apply(@Nonnull final DataProcessInstanceRunResult input) {

        final com.linkedin.datahub.graphql.generated.DataProcessInstanceRunResult result =
                new com.linkedin.datahub.graphql.generated.DataProcessInstanceRunResult();

        if (input.hasType()) {
            result.setResultType(DataProcessInstanceRunResultType.valueOf(input.getType().toString()));
        }

        if (input.hasNativeResultType()) {
            result.setNativeResultType(input.getNativeResultType());
        }

        return result;
    }
}
