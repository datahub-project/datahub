package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.generated.PropertyValueInput;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.structured.StructuredPropertyValueAssignment;

import javax.annotation.Nonnull;

public class PropertyValueInputMapper implements ModelMapper<PropertyValueInput, StructuredPropertyValueAssignment> {

    public static final PropertyValueInputMapper INSTANCE =
            new PropertyValueInputMapper();

    public static StructuredPropertyValueAssignment map(@Nonnull final PropertyValueInput input) {
        return INSTANCE.apply(input);
    }

    @Override
    public StructuredPropertyValueAssignment apply(@Nonnull final PropertyValueInput input) {
        //TODO Implement this
        return null;
    }
}
