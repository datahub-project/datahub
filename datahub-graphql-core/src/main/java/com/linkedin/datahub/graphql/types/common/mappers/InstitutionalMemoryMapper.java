package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.generated.InstitutionalMemory;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class InstitutionalMemoryMapper implements ModelMapper<com.linkedin.common.InstitutionalMemory, InstitutionalMemory> {

    public static final InstitutionalMemoryMapper INSTANCE = new InstitutionalMemoryMapper();

    public static InstitutionalMemory map(@Nonnull final com.linkedin.common.InstitutionalMemory memory) {
        return INSTANCE.apply(memory);
    }

    @Override
    public InstitutionalMemory apply(@Nonnull final com.linkedin.common.InstitutionalMemory input) {
        final InstitutionalMemory result = new InstitutionalMemory();
        result.setElements(input.getElements().stream().map(InstitutionalMemoryMetadataMapper::map).collect(Collectors.toList()));
        return result;
    }
}
