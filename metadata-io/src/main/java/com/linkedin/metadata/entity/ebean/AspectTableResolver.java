package com.linkedin.metadata.entity.ebean;

import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/**
 * Resolves the qualified table name to splice into raw-SQL statements built outside Ebean's ORM
 * query layer. Ebean's {@code Database.find(...)} calls are already table-agnostic (governed by
 * {@code DatabaseConfig}); this resolver only matters for the handful of hand-built {@code
 * RawSql}/{@code SqlQuery} statements that reference a table name literally.
 *
 * <p>Takes the unqualified base table name (e.g. {@code "metadata_aspect_v2"}) so a single bean
 * serves every raw-SQL table in the deployment; an extension module qualifies it per-request
 * (e.g. per-deployment catalog) without the caller knowing how.
 */
public interface AspectTableResolver {

  /**
   * Resolve the (optionally qualified) table name to splice into raw SQL.
   *
   * @param opContext operation context for the current call
   * @param baseTableName unqualified base table name, e.g. {@code "metadata_aspect_v2"}
   * @return table reference, e.g. {@code " metadata_aspect_v2 "}
   */
  @Nonnull
  String aspectTable(@Nonnull OperationContext opContext, @Nonnull String baseTableName);
}
