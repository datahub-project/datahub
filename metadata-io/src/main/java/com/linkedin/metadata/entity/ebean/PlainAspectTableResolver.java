package com.linkedin.metadata.entity.ebean;

import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/**
 * OSS default {@link AspectTableResolver}: returns the unqualified base table name padded with a
 * single leading and trailing space (e.g. {@code " metadata_aspect_v2 "}), matching the {@link
 * AspectTableResolver} contract — raw-SQL call sites concatenate {@code FROM}/{@code WHERE}
 * fragments that depend on this padding. Registered as a bean via {@code EntityAspectDaoFactory};
 * an extension module may override with a {@code @Primary} bean to qualify the table per-request.
 */
public class PlainAspectTableResolver implements AspectTableResolver {

  @Override
  @Nonnull
  public String aspectTable(@Nonnull OperationContext opContext, @Nonnull String baseTableName) {
    return " " + baseTableName + " ";
  }
}
