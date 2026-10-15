package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataTransform;
import com.linkedin.datahub.graphql.generated.DataTransformLogic;
import com.linkedin.datahub.graphql.generated.QueryLanguage;
import com.linkedin.datahub.graphql.generated.QueryStatement;
import com.linkedin.metadata.authorization.EntityAspectAuthorizationUtils;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Not a {@code ModelMapper}: unlike most aspect mappers, this one needs the owning entity's URN (to
 * decide whether {@code queryStatement}'s SQL text may be shown) in addition to the aspect content,
 * so it takes a 3-arg signature instead — same reasoning as {@code SchemaMapper}.
 */
public class DataTransformLogicMapper {

  public static final DataTransformLogicMapper INSTANCE = new DataTransformLogicMapper();

  public static DataTransformLogic map(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.common.DataTransformLogic input,
      @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(context, input, entityUrn);
  }

  public DataTransformLogic apply(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.common.DataTransformLogic input,
      @Nonnull final Urn entityUrn) {

    final DataTransformLogic result = new DataTransformLogic();

    // Map transforms array using DataTransformMapper
    result.setTransforms(
        input.getTransforms().stream()
            .map(transform -> DataTransformMapper.map(context, transform, entityUrn))
            .collect(Collectors.toList()));

    return result;
  }
}

class DataTransformMapper {

  public static final DataTransformMapper INSTANCE = new DataTransformMapper();

  public static DataTransform map(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.common.DataTransform input,
      @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(context, input, entityUrn);
  }

  /**
   * {@code queryStatement} carries the transform's SQL text, so it is withheld — same as a Query
   * entity's SQL — unless {@link EntityAspectAuthorizationUtils#canViewQueriesOnEntity} grants it;
   * a {@code null} context is treated as unrestricted.
   */
  public DataTransform apply(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.common.DataTransform input,
      @Nonnull final Urn entityUrn) {

    final DataTransform result = new DataTransform();

    if (input.hasQueryStatement()
        && (context == null
            || EntityAspectAuthorizationUtils.canViewQueriesOnEntity(
                context.getOperationContext(), entityUrn))) {
      QueryStatement statement =
          new QueryStatement(
              input.getQueryStatement().getValue(),
              QueryLanguage.valueOf(input.getQueryStatement().getLanguage().toString()));
      result.setQueryStatement(statement);
    }

    return result;
  }
}
