package com.linkedin.datahub.graphql.types.common.mappers;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.SiblingProperties;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class SiblingsMapper
    implements ModelMapper<com.linkedin.common.Siblings, SiblingProperties> {

  public static final SiblingsMapper INSTANCE = new SiblingsMapper();

  public static SiblingProperties map(
      @Nullable QueryContext context, @Nonnull final com.linkedin.common.Siblings siblings) {
    return INSTANCE.apply(context, siblings);
  }

  @Override
  public SiblingProperties apply(
      @Nullable QueryContext context, @Nonnull final com.linkedin.common.Siblings siblings) {
    final SiblingProperties result = new SiblingProperties();
    result.setIsPrimary(siblings.isPrimary());
    result.setSiblings(
        siblings.getSiblings().stream()
            .filter(s -> context == null | canView(context.getOperationContext(), s))
            .map(s -> UrnToEntityMapper.map(context, s))
            .collect(Collectors.toList()));
    return result;
  }
}
