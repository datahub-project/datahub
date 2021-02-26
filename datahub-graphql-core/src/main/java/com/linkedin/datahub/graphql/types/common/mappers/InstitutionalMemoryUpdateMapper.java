package com.linkedin.datahub.graphql.types.common.mappers;

import java.util.stream.Collectors;

import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.InstitutionalMemoryMetadataArray;
import com.linkedin.datahub.graphql.generated.InstitutionalMemoryUpdate;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import lombok.NonNull;

public class InstitutionalMemoryUpdateMapper implements ModelMapper<InstitutionalMemoryUpdate, InstitutionalMemory> {

    private static final InstitutionalMemoryUpdateMapper INSTANCE = new InstitutionalMemoryUpdateMapper();

    public static InstitutionalMemory map(@NonNull final InstitutionalMemoryUpdate input) {
        return INSTANCE.apply(input);
    }

    @Override
    public InstitutionalMemory apply(@NonNull final InstitutionalMemoryUpdate input) {
        final InstitutionalMemory institutionalMemory = new InstitutionalMemory();
        institutionalMemory.setElements(new InstitutionalMemoryMetadataArray(
            input.getElements()
                .stream()
                .map(InstitutionalMemoryMetadataUpdateMapper::map)
                .collect(Collectors.toList())));
        return institutionalMemory;
    }
}
