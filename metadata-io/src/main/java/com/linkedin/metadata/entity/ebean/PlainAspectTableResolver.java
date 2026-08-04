package com.linkedin.metadata.entity.ebean;

import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/**
 * OSS default {@link AspectTableResolver}: always the unqualified {@code metadata_aspect_v2} table.
 * Registered as a bean via {@code EntityAspectDaoFactory}; an extension module may override with a
 * {@code @Primary} bean to qualify the table per-request.
 */
public class PlainAspectTableResolver implements AspectTableResolver {

  @Override
  @Nonnull
  public String aspectTable(@Nonnull OperationContext opContext) {
    return " metadata_aspect_v2 ";
  }
}
