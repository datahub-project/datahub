package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.generated.SiblingProperties;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class SiblingsMapper implements ModelMapper<com.linkedin.common.Siblings, SiblingProperties> {

  public static final SiblingsMapper INSTANCE = new SiblingsMapper();

  public static SiblingProperties map(@Nonnull final com.linkedin.common.Siblings siblings) {
    return INSTANCE.apply(siblings);
  }

  @Override
  public SiblingProperties apply(@Nonnull final com.linkedin.common.Siblings siblings) {
    final SiblingProperties result = new SiblingProperties();
    result.setIsPrimary(siblings.isPrimary());
    result.setSiblings(siblings.getSiblings()
        .stream()
        .map(UrnToEntityMapper::map)
        .collect(Collectors.toList()));
    return result;
  }
}
