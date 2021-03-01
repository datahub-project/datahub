package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.generated.ResultsType;
import com.linkedin.datahub.graphql.generated.StringBox;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import lombok.NonNull;

public class ResultsTypeMapper implements ModelMapper<com.linkedin.ml.metadata.ResultsType, ResultsType> {

    public static final ResultsTypeMapper INSTANCE = new ResultsTypeMapper();

    public static ResultsType map(@NonNull final com.linkedin.ml.metadata.ResultsType input) {
        return INSTANCE.apply(input);
    }

    @Override
    public ResultsType apply(@NonNull final com.linkedin.ml.metadata.ResultsType input) {
        ResultsType result = null;
        if(input.isString()) {
            result = new StringBox(input.getString());
        }
        return result;
    }
}
