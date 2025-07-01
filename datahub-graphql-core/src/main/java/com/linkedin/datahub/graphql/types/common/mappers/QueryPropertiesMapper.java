package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.GetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.datahub.graphql.types.mappers.EmbeddedModelMapper;
import com.linkedin.query.QueryProperties;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class QueryPropertiesMapper
    implements EmbeddedModelMapper<
        QueryProperties, com.linkedin.datahub.graphql.generated.QueryProperties> {

  public static final QueryPropertiesMapper INSTANCE = new QueryPropertiesMapper();

  public static com.linkedin.datahub.graphql.generated.QueryProperties map(
      @Nullable final QueryContext context,
      @Nonnull final QueryProperties input,
      @Nonnull Urn entityUrn) {
    return INSTANCE.apply(context, input, entityUrn);
  }

  @Override
  public com.linkedin.datahub.graphql.generated.QueryProperties apply(
      @Nullable final QueryContext context,
      @Nonnull final QueryProperties input,
      @Nonnull Urn entityUrn) {

    final com.linkedin.datahub.graphql.generated.QueryProperties result =
        new com.linkedin.datahub.graphql.generated.QueryProperties();

    // Map Query Source
    result.setSource(QuerySource.valueOf(input.getSource().toString()));

    // Map Query Statement
    result.setStatement(
        new QueryStatement(
            input.getStatement().getValue(),
            QueryLanguage.valueOf(input.getStatement().getLanguage().toString())));

    // Map optional fields
    result.setName(input.getName(GetMode.NULL));
    result.setDescription(input.getDescription(GetMode.NULL));

    // Map origin if present
    if (input.hasOrigin() && input.getOrigin() != null) {
      result.setOrigin(UrnToEntityMapper.map(context, input.getOrigin()));
    }

    // Map created audit stamp
    AuditStamp created = new AuditStamp();
    created.setTime(input.getCreated().getTime());
    created.setActor(input.getCreated().getActor(GetMode.NULL).toString());
    result.setCreated(created);

    // Map last modified audit stamp
    AuditStamp lastModified = new AuditStamp();
    lastModified.setTime(input.getLastModified().getTime());
    lastModified.setActor(input.getLastModified().getActor(GetMode.NULL).toString());
    result.setLastModified(lastModified);

    result.setCustomProperties(CustomPropertiesMapper.map(input.getCustomProperties(), entityUrn));

    return result;
  }
}
