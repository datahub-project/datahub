package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.StructuredPropertyInputParams;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredPropertyValueAssignment;

import javax.annotation.Nonnull;
import java.net.URISyntaxException;

public class StructuredPropertyInputParamsMapper
        implements ModelMapper<StructuredPropertyInputParams, StructuredPropertyValueAssignment> {

    public static final StructuredPropertyInputParamsMapper INSTANCE =
            new StructuredPropertyInputParamsMapper();

    public static StructuredPropertyValueAssignment map(@Nonnull final StructuredPropertyInputParams input) {
        return INSTANCE.apply(input);
    }

    @Override
    public StructuredPropertyValueAssignment apply(@Nonnull final StructuredPropertyInputParams input) {
        StructuredPropertyValueAssignment result = new StructuredPropertyValueAssignment();
        try {
            result.setPropertyUrn(Urn.createFromString(input.getStructuredPropertyUrn()));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        PrimitivePropertyValueArray array = new PrimitivePropertyValueArray();
        input.getValues().forEach(value -> {
                    array.add(PropertyValueInputMapper.map(value));
        });
        //TODO Continue here
        result.setValues(PropertyValueInputMapper.map());
        return result;
    }
}
