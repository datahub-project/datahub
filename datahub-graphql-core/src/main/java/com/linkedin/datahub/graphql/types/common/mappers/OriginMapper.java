package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Origin;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class OriginMapper {

  public static final OriginMapper INSTANCE = new OriginMapper();

  public static Origin map(
      @Nullable QueryContext context, @Nonnull final com.linkedin.common.Origin origin) {
    return INSTANCE.apply(context, origin);
  }

  public Origin apply(
      @Nullable QueryContext context, @Nonnull final com.linkedin.common.Origin origin) {
    final Origin result = new Origin();
    if (origin.hasType()) {
      result.setType(
          com.linkedin.datahub.graphql.generated.OriginType.valueOf(origin.getType().toString()));
    } else {
      result.setType(com.linkedin.datahub.graphql.generated.OriginType.UNKNOWN);
    }
    if (origin.hasExternalType()) {
      result.setExternalType(origin.getExternalType());
    }
    return result;
  }
}
