/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.jdbc.JdbcDatasetUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.sql.ColumnLineage;
import io.openlineage.sql.ColumnMeta;
import io.openlineage.sql.DbTableMeta;
import io.openlineage.sql.ExtractionError;
import io.openlineage.sql.OpenLineageSql;
import io.openlineage.sql.SqlMeta;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions$;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

@Slf4j
public class JdbcSparkUtils {

  public static <D extends OpenLineage.Dataset> List<D> getDatasets(
      DatasetFactory<D> datasetFactory, SqlMeta meta, JDBCRelation relation) {

    StructType schema = relation.schema();
    String jdbcUrl = relation.jdbcOptions().url();
    Properties jdbcProperties = relation.jdbcOptions().asConnectionProperties();

    if (meta.columnLineage().isEmpty()) {
      int numberOfTables = meta.inTables().size();

      return meta.inTables().stream()
          .map(
              dbtm -> {
                DatasetIdentifier di =
                    JdbcDatasetUtils.getDatasetIdentifier(
                        jdbcUrl, dbtm.qualifiedName(), jdbcProperties);

                if (numberOfTables > 1) {
                  return datasetFactory.getDataset(di.getName(), di.getNamespace());
                }

                return datasetFactory.getDataset(di.getName(), di.getNamespace(), schema);
              })
          .collect(Collectors.toList());
    }
    return meta.inTables().stream()
        .map(
            dbtm -> {
              DatasetIdentifier di =
                  JdbcDatasetUtils.getDatasetIdentifier(
                      jdbcUrl, dbtm.qualifiedName(), jdbcProperties);
              return datasetFactory.getDataset(
                  di.getName(), di.getNamespace(), generateSchemaFromSqlMeta(dbtm, schema, meta));
            })
        .collect(Collectors.toList());
  }

  public static StructType generateSchemaFromSqlMeta(
      DbTableMeta origin, StructType schema, SqlMeta sqlMeta) {
    StructType originSchema = new StructType();
    for (StructField f : schema.fields()) {
      List<ColumnMeta> fields =
          sqlMeta.columnLineage().stream()
              .filter(cl -> cl.descendant().name().equals(f.name()))
              .flatMap(
                  cl ->
                      cl.lineage().stream()
                          .filter(
                              cm -> cm.origin().isPresent() && cm.origin().get().equals(origin)))
              .collect(Collectors.toList());
      for (ColumnMeta cm : fields) {
        originSchema = originSchema.add(cm.name(), f.dataType());
      }
    }
    return originSchema;
  }

  public static Optional<SqlMeta> extractQueryFromSpark(JDBCRelation relation) {
    Optional<String> table =
        ScalaConversionUtils.asJavaOptional(
            relation.jdbcOptions().parameters().get(JDBCOptions$.MODULE$.JDBC_TABLE_NAME()));
    // in some cases table value can be "(SELECT col1, col2 FROM table_name WHERE some='filter')
    // ALIAS"
    if (table.isPresent() && !table.get().startsWith("(")) {
      DbTableMeta origin = new DbTableMeta(null, null, table.get());
      return Optional.of(
          new SqlMeta(
              Collections.singletonList(origin),
              Collections.emptyList(),
              Arrays.stream(relation.schema().fields())
                  .map(
                      field ->
                          new ColumnLineage(
                              new ColumnMeta(null, field.name()),
                              Collections.singletonList(new ColumnMeta(origin, field.name()))))
                  .collect(Collectors.toList()),
              Collections.emptyList()));
    }

    String query = queryStringFromJdbcOptions(relation.jdbcOptions());

    String dialect = extractDialectFromJdbcUrl(relation.jdbcOptions().url());
    Optional<SqlMeta> sqlMeta = OpenLineageSql.parse(Collections.singletonList(query), dialect);

    if (!sqlMeta.isPresent()) { // missing JNI library
      return sqlMeta;
    }
    if (!sqlMeta.get().errors().isEmpty()) { // error return nothing
      log.error(
          String.format(
              "error while parsing query: %s",
              sqlMeta.get().errors().stream()
                  .map(ExtractionError::toString)
                  .collect(Collectors.joining(","))));
      return Optional.empty();
    }
    if (sqlMeta.get().inTables().isEmpty()) {
      log.error("no tables defined in query, this should not happen");
      return Optional.empty();
    }
    return sqlMeta;
  }

  public static String queryStringFromJdbcOptions(JDBCOptions options) {
    String tableOrQuery = options.tableOrQuery();
    return tableOrQuery.substring(0, tableOrQuery.lastIndexOf(")")).replaceFirst("\\(", "");
  }

  private static String extractDialectFromJdbcUrl(String jdbcUrl) {
    Pattern pattern = Pattern.compile("^jdbc:([^:]+):.*");
    Matcher matcher = pattern.matcher(jdbcUrl);

    if (matcher.find()) {
      String dialect = matcher.group(1);
      // Added to map SQL Server dialect to the one recognized by OpenLineage
      return "sqlserver".equals(dialect) ? "mssql" : dialect;
    } else {
      return null;
    }
  }
}
