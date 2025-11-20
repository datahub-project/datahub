/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE;
import static io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.jdbc.JdbcDatasetUtils;
import io.openlineage.spark.agent.util.DatasetFacetsUtils;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanDatasetBuilder;
import io.openlineage.spark.api.JobNameSuffixProvider;
import io.openlineage.spark.api.OpenLineageContext;
import java.net.URI;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.sources.SchemaRelationProvider;
import org.apache.spark.sql.types.StructType;
import scala.Option;

/**
 * {@link LogicalPlan} visitor that matches an {@link SaveIntoDataSourceCommand} and extracts the
 * output {@link OpenLineage.Dataset} being written. Since the output datasource is a {@link
 * BaseRelation}, we wrap it with an artificial {@link LogicalRelation} so we can delegate to other
 * plan visitors.
 */
@Slf4j
public class SaveIntoDataSourceCommandVisitor
    extends AbstractQueryPlanDatasetBuilder<
        SparkListenerEvent, SaveIntoDataSourceCommand, OutputDataset>
    implements JobNameSuffixProvider<SaveIntoDataSourceCommand> {

  public SaveIntoDataSourceCommandVisitor(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    if (context.getSparkSession().isPresent() && x instanceof SaveIntoDataSourceCommand) {
      SaveIntoDataSourceCommand command = (SaveIntoDataSourceCommand) x;
      if (PlanUtils.safeIsInstanceOf(
          command.dataSource(), "com.google.cloud.spark.bigquery.BigQueryRelationProvider")) {
        return false;
      }
      return command.dataSource() instanceof SchemaRelationProvider
          || context.getSparkExtensionVisitorWrapper().isDefinedAt(command.dataSource())
          || command.dataSource() instanceof RelationProvider;
    }
    return false;
  }

  @Override
  public boolean isDefinedAt(SparkListenerEvent x) {
    return super.isDefinedAt(x)
        && context
            .getQueryExecution()
            .filter(qe -> isDefinedAtLogicalPlan(qe.optimizedPlan()))
            .isPresent();
  }

  @Override
  public List<OutputDataset> apply(SaveIntoDataSourceCommand cmd) {
    // intentionally unimplemented
    throw new UnsupportedOperationException("apply(LogicalPlay) is not implemented");
  }

  @Override
  @SuppressWarnings("PMD.AvoidDuplicateLiterals")
  public List<OutputDataset> apply(SparkListenerEvent event, SaveIntoDataSourceCommand command) {
    BaseRelation relation;

    if (context.getSparkExtensionVisitorWrapper().isDefinedAt(command.dataSource())) {
      DatasetIdentifier datasetIdentifier =
          context
              .getSparkExtensionVisitorWrapper()
              .getLineageDatasetIdentifier(
                  command.dataSource(),
                  event.getClass().getName(),
                  context.getSparkSession().get().sqlContext(),
                  command.options());

      return datasetIdentifier != null
          ? Collections.singletonList(
              outputDataset().getDataset(datasetIdentifier, getSchema(command)))
          : Collections.emptyList();
    }

    // Kafka has some special handling because the Source and Sink relations require different
    // options. A KafkaRelation for writes uses the "topic" option, while the same relation for
    // reads requires the "subscribe" option. The KafkaSourceProvider never returns a KafkaRelation
    // for write operations (it executes the real writer, then returns a dummy relation), so we have
    // to use it to construct a reader, meaning we need to change the "topic" option to "subscribe".
    // Since it requires special handling anyway, we just go ahead and extract the Dataset(s)
    // directly.
    // TODO- it may be the case that we need to extend this pattern to support arbitrary relations,
    // as other impls of CreatableRelationProvider may not be able to be handled in the generic way.
    if (KafkaRelationVisitor.isKafkaSource(command.dataSource())) {
      return KafkaRelationVisitor.createKafkaDatasets(
          outputDataset(),
          command.dataSource(),
          command.options(),
          command.mode(),
          command.schema());
    }

    // Similar to Kafka, Azure Kusto also has some special handling. So we use the method
    // below for extracting the dataset from Kusto write operations.
    if (KustoRelationVisitor.isKustoSource(command.dataSource())) {
      return KustoRelationVisitor.createKustoDatasets(
          outputDataset(), command.options(), command.schema());
    }

    StructType schema = getSchema(command);
    LifecycleStateChange lifecycleStateChange =
        (SaveMode.Overwrite == command.mode()) ? OVERWRITE : CREATE;

    if (command.dataSource().getClass().getName().contains("DeltaDataSource")) {
      // Handle path-based Delta tables
      if (command.options().contains("path")) {
        URI uri = URI.create(command.options().get("path").get());
        return Collections.singletonList(
            outputDataset().getDataset(PathUtils.fromURI(uri), schema, lifecycleStateChange));
      }

      // Handle catalog-based Delta tables (saveAsTable scenarios)
      if (command.options().contains("table")) {
        String tableName = command.options().get("table").get();
        // For catalog tables, use the default namespace or catalog
        String namespace = "spark_catalog"; // Default Spark catalog namespace
        DatasetIdentifier identifier = new DatasetIdentifier(tableName, namespace);
        return Collections.singletonList(
            outputDataset().getDataset(identifier, schema, lifecycleStateChange));
      }

      // Handle saveAsTable without explicit table option - check for table info in query execution
      if (context.getQueryExecution().isPresent()) {
        QueryExecution qe = context.getQueryExecution().get();
        // Try to extract table name from query execution context
        String extractedTableName = extractTableNameFromContext(qe);
        if (extractedTableName != null) {
          String namespace = "spark_catalog";
          DatasetIdentifier identifier = new DatasetIdentifier(extractedTableName, namespace);
          return Collections.singletonList(
              outputDataset().getDataset(identifier, schema, lifecycleStateChange));
        }
      }

      log.debug(
          "Delta table detected but could not determine path or table name from options: {}",
          command.options());
    }

    if (command
        .dataSource()
        .getClass()
        .getCanonicalName()
        .equals(JdbcRelationProvider.class.getCanonicalName())) {
      String tableName = command.options().get("dbtable").get();
      String url = command.options().get("url").get();
      DatasetIdentifier identifier =
          JdbcDatasetUtils.getDatasetIdentifier(url, tableName, new Properties());
      return Collections.singletonList(
          outputDataset().getDataset(identifier, schema, lifecycleStateChange));
    }

    SQLContext sqlContext = context.getSparkSession().get().sqlContext();
    try {
      if (command.dataSource() instanceof RelationProvider) {
        RelationProvider p = (RelationProvider) command.dataSource();
        relation = p.createRelation(sqlContext, command.options());
      } else {
        SchemaRelationProvider p = (SchemaRelationProvider) command.dataSource();
        relation = p.createRelation(sqlContext, command.options(), schema);
      }
    } catch (Exception ex) {
      // Bad detection of errors in scala
      if (ex instanceof SQLException) {
        // This can happen on SparkListenerSQLExecutionStart for example for sqlite, when database
        // does not exist yet - it will be created as command execution
        // Still, we can just ignore it on start, because it will work on end
        // see SparkReadWriteIntegTest.testReadFromFileWriteToJdbc
        log.warn("Can't create relation: ", ex);
        return Collections.emptyList();
      }
      throw ex;
    }
    LogicalRelation logicalRelation =
        new LogicalRelation(
            relation,
            ScalaConversionUtils.asScalaSeqEmpty(),
            Option.empty(),
            command.isStreaming());
    return delegate(
            context.getOutputDatasetQueryPlanVisitors(), context.getOutputDatasetBuilders(), event)
        .applyOrElse(
            logicalRelation,
            ScalaConversionUtils.toScalaFn((lp) -> Collections.<OutputDataset>emptyList()))
        .stream()
        // constructed datasets don't include the output stats, so add that facet here
        .map(
            ds -> {
              Builder<String, OpenLineage.DatasetFacet> facetsMap =
                  ImmutableMap.<String, OpenLineage.DatasetFacet>builder();
              if (ds.getFacets().getAdditionalProperties() != null) {
                facetsMap.putAll(ds.getFacets().getAdditionalProperties());
              }
              ds.getFacets().getAdditionalProperties().putAll(facetsMap.build());

              // rebuild whole dataset with a LifecycleStateChange facet added
              OpenLineage.DatasetFacets facets =
                  DatasetFacetsUtils.copyToBuilder(context, ds.getFacets())
                      .lifecycleStateChange(
                          context
                              .getOpenLineage()
                              .newLifecycleStateChangeDatasetFacet(
                                  OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange
                                      .OVERWRITE,
                                  null))
                      .build();

              OpenLineage.OutputDataset newDs =
                  context
                      .getOpenLineage()
                      .newOutputDataset(
                          ds.getNamespace(), ds.getName(), facets, ds.getOutputFacets());
              return newDs;
            })
        .collect(Collectors.toList());
  }

  private StructType getSchema(SaveIntoDataSourceCommand command) {
    StructType schema = command.schema();
    if ((schema == null || schema.fields() == null || schema.fields().length == 0)
        && command.query() != null
        && command.query().output() != null) {
      // get schema from logical plan's output
      schema = PlanUtils.toStructType(ScalaConversionUtils.fromSeq(command.query().output()));
    }
    return schema;
  }

  /**
   * Attempts to extract table name from QueryExecution context for saveAsTable operations. This
   * handles cases where the table name isn't explicitly in the command options.
   */
  private String extractTableNameFromContext(QueryExecution qe) {
    try {
      // Try to get table name from SQL text if available
      // Note: sqlText() is not available in all Spark versions, use reflection
      try {
        java.lang.reflect.Method sqlTextMethod = qe.getClass().getMethod("sqlText");
        Object sqlOption = sqlTextMethod.invoke(qe);
        if (sqlOption != null && ((Option<?>) sqlOption).isDefined()) {
          String sql = (String) ((Option<?>) sqlOption).get();
          log.debug("Attempting to extract table name from SQL: {}", sql);

          // Look for saveAsTable pattern which typically generates CREATE TABLE statements
          if (sql.toLowerCase().contains("create table")) {
            // Extract table name using regex pattern matching
            String[] tokens = sql.split("\\s+");
            for (int i = 0; i < tokens.length - 1; i++) {
              if (tokens[i].toLowerCase().equals("table")) {
                String candidateTableName = tokens[i + 1];
                // Clean up table name (remove backticks, quotes, database prefix)
                candidateTableName = candidateTableName.replaceAll("[`'\"]", "");
                // Handle database.table format by taking just the table name
                if (candidateTableName.contains(".")) {
                  String[] parts = candidateTableName.split("\\.");
                  candidateTableName = parts[parts.length - 1]; // Take the last part (table name)
                }
                if (!candidateTableName.isEmpty()
                    && !candidateTableName.toLowerCase().equals("if")) {
                  log.debug("Extracted table name from SQL: {}", candidateTableName);
                  return candidateTableName;
                }
              }
            }
          }
        }
      } catch (Exception reflectionEx) {
        log.debug(
            "sqlText() method not available in this Spark version: {}", reflectionEx.getMessage());
      }

      log.debug("Could not extract table name from QueryExecution SQL text");
    } catch (Exception e) {
      log.debug("Error extracting table name from QueryExecution: {}", e.getMessage());
    }

    return null;
  }

  @Override
  public Optional<String> jobNameSuffix(OpenLineageContext context) {
    return context
        .getQueryExecution()
        .map(QueryExecution::optimizedPlan)
        .filter(p -> p instanceof SaveIntoDataSourceCommand)
        .map(p -> (SaveIntoDataSourceCommand) p)
        .map(p -> jobNameSuffix(p))
        .filter(Optional::isPresent)
        .map(Optional::get);
  }

  @SuppressWarnings("PMD.AvoidDuplicateLiterals")
  public Optional<String> jobNameSuffix(SaveIntoDataSourceCommand command) {
    if (command.dataSource().getClass().getName().contains("DeltaDataSource")
        && command.options().contains("path")) {
      return Optional.of(trimPath(command.options().get("path").get()));
    } else if (KustoRelationVisitor.isKustoSource(command.dataSource())) {
      return Optional.ofNullable(command.options().get("kustotable"))
          .filter(Option::isDefined)
          .map(Option::get);
    } else if (command.options().get("table").isDefined()) {
      return Optional.of(command.options().get("table").get());
    } else if (command.dataSource() instanceof RelationProvider
        || command.dataSource() instanceof SchemaRelationProvider) {
      return ScalaConversionUtils.fromMap(command.options()).keySet().stream()
          .filter(key -> key.toLowerCase(Locale.ROOT).contains("table"))
          .findAny()
          .map(key -> command.options().get(key).get());
    }

    return Optional.empty();
  }
}
