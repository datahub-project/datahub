package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.InstitutionalMemory;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class InstitutionalMemoryMapper {

  public static final InstitutionalMemoryMapper INSTANCE = new InstitutionalMemoryMapper();

  public static InstitutionalMemory map(
      @Nonnull final com.linkedin.common.InstitutionalMemory memory, @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(memory, entityUrn);
  }

  public InstitutionalMemory apply(
      @Nonnull final com.linkedin.common.InstitutionalMemory input, @Nonnull final Urn entityUrn) {
    final InstitutionalMemory result = new InstitutionalMemory();
    result.setElements(
        input.getElements().stream()
            .map(metadata -> InstitutionalMemoryMetadataMapper.map(metadata, entityUrn))
            .collect(Collectors.toList()));
    return result;
  }
}
