package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.InstitutionalMemory;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class InstitutionalMemoryMapper {

  public static final InstitutionalMemoryMapper INSTANCE = new InstitutionalMemoryMapper();

  public static InstitutionalMemory map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.common.InstitutionalMemory memory,
      @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(context, memory, entityUrn);
  }

  public InstitutionalMemory apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.common.InstitutionalMemory input,
      @Nonnull final Urn entityUrn) {
    final InstitutionalMemory result = new InstitutionalMemory();
    result.setElements(
        input.getElements().stream()
            .map(metadata -> InstitutionalMemoryMetadataMapper.map(context, metadata, entityUrn))
            .collect(Collectors.toList()));
    return result;
  }
}
