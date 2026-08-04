package com.linkedin.metadata.entity.ebean;

import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/**
 * Resolves the qualified {@code metadata_aspect_v2} table name for raw-SQL statements built outside
 * Ebean's ORM query layer. Ebean's {@code Database.find(EbeanAspectV2.class)} calls are already
 * table-agnostic (governed by {@code DatabaseConfig}); this resolver only matters for the handful
 * of hand-built {@code RawSql}/{@code SqlQuery} statements in {@link EbeanAspectDao} that reference
 * the table name literally.
 */
public interface AspectTableResolver {

  /**
   * Resolve the (optionally qualified) table name to splice into raw SQL.
   *
   * @param opContext operation context for the current call
   * @return table reference, e.g. {@code " metadata_aspect_v2 "}
   */
  @Nonnull
  String aspectTable(@Nonnull OperationContext opContext);
}
