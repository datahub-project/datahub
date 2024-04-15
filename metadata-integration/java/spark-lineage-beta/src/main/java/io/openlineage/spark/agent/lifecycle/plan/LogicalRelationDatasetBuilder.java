/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.lifecycle.plan.handlers.JdbcRelationHandler;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.AbstractQueryPlanDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import scala.collection.JavaConversions;

/**
 * {@link LogicalPlan} visitor that attempts to extract a {@link OpenLineage.Dataset} from a {@link
 * LogicalRelation}. The {@link org.apache.spark.sql.sources.BaseRelation} is tested for known
 * types, such as {@link HadoopFsRelation} or {@link JDBCRelation}s, as those are easy to extract
 * exact dataset information.
 *
 * <p>For {@link HadoopFsRelation}s, it is assumed that a single directory maps to a single {@link
 * OpenLineage.Dataset}. Any files referenced are replaced by their parent directory and all files
 * in a given directory are assumed to belong to the same {@link OpenLineage.Dataset}. Directory
 * partitioning is currently not addressed.
 *
 * <p>For {@link JDBCRelation}s, {@link OpenLineage.Dataset} naming expects the namespace to be the
 * JDBC connection URL (schema and authority only) and the table name to be the <code>
 * &lt;database&gt;
 * </code>.<code>&lt;tableName&gt;</code>.
 *
 * <p>{@link CatalogTable}s, if present, can be used to describe the {@link OpenLineage.Dataset} if
 * its {@link org.apache.spark.sql.sources.BaseRelation} is unknown.
 *
 * <p>TODO If a user specifies the {@link JDBCOptions#JDBC_QUERY_STRING()} option, we do not parse
 * the sql to determine the specific tables used. Since we return a List of {@link
 * OpenLineage.Dataset}s, we can parse the sql and determine each table referenced to return a
 * complete list of datasets referenced.
 */
@Slf4j
public class LogicalRelationDatasetBuilder<D extends OpenLineage.Dataset>
    extends AbstractQueryPlanDatasetBuilder<SparkListenerEvent, LogicalRelation, D> {

  private final DatasetFactory<D> datasetFactory;

  public LogicalRelationDatasetBuilder(
      OpenLineageContext context, DatasetFactory<D> datasetFactory, boolean searchDependencies) {
    super(context, searchDependencies);
    this.datasetFactory = datasetFactory;
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    // if a LogicalPlan is a single node plan like `select * from temp`,
    // then it's leaf node and should not be considered output node
    if (x instanceof LogicalRelation && isSingleNodeLogicalPlan(x) && !searchDependencies) {
      return false;
    }

    return x instanceof LogicalRelation
        && (((LogicalRelation) x).relation() instanceof HadoopFsRelation
            || ((LogicalRelation) x).relation() instanceof JDBCRelation
            || ((LogicalRelation) x).catalogTable().isDefined());
  }

  private boolean isSingleNodeLogicalPlan(LogicalPlan x) {
    return context
            .getQueryExecution()
            .map(qe -> qe.optimizedPlan())
            .filter(p -> p.equals(x))
            .isPresent()
        && (x.children() == null || x.children().isEmpty());
  }

  @Override
  public List<D> apply(LogicalRelation logRel) {
    if (logRel.catalogTable() != null && logRel.catalogTable().isDefined()) {
      return handleCatalogTable(logRel);
    } else if (logRel.relation() instanceof HadoopFsRelation) {
      return handleHadoopFsRelation(logRel);
    } else if (logRel.relation() instanceof JDBCRelation) {
      return new JdbcRelationHandler<>(datasetFactory).handleRelation(logRel);
    }
    throw new IllegalArgumentException(
        "Expected logical plan to be either HadoopFsRelation, JDBCRelation, "
            + "or CatalogTable but was "
            + logRel);
  }

  private List<D> handleCatalogTable(LogicalRelation logRel) {
    CatalogTable catalogTable = logRel.catalogTable().get();

    DatasetIdentifier di = PathUtils.fromCatalogTable(catalogTable);

    OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
        context.getOpenLineage().newDatasetFacetsBuilder();
    datasetFacetsBuilder.schema(PlanUtils.schemaFacet(context.getOpenLineage(), logRel.schema()));
    datasetFacetsBuilder.dataSource(
        PlanUtils.datasourceFacet(context.getOpenLineage(), di.getNamespace()));

    getDatasetVersion(logRel)
        .map(
            version ->
                datasetFacetsBuilder.version(
                    context.getOpenLineage().newDatasetVersionDatasetFacet(version)));

    return Collections.singletonList(datasetFactory.getDataset(di, datasetFacetsBuilder));
  }

  private List<D> handleHadoopFsRelation(LogicalRelation x) {
    HadoopFsRelation relation = (HadoopFsRelation) x.relation();
    try {
      return context
          .getSparkSession()
          .map(
              session -> {
                Configuration hadoopConfig =
                    session.sessionState().newHadoopConfWithOptions(relation.options());

                DatasetFacetsBuilder datasetFacetsBuilder =
                    context.getOpenLineage().newDatasetFacetsBuilder();
                getDatasetVersion(x)
                    .map(
                        version ->
                            datasetFacetsBuilder.version(
                                context.getOpenLineage().newDatasetVersionDatasetFacet(version)));

                Collection<Path> rootPaths =
                    JavaConversions.asJavaCollection(relation.location().rootPaths());

                if (isSingleFileRelation(rootPaths, hadoopConfig)) {
                  return Collections.singletonList(
                      datasetFactory.getDataset(
                          rootPaths.stream().findFirst().get().toUri(),
                          relation.schema(),
                          datasetFacetsBuilder));
                } else {
                  return rootPaths.stream()
                      .map(p -> PlanUtils.getDirectoryPath(p, hadoopConfig))
                      .distinct()
                      .map(
                          p -> {
                            // TODO- refactor this to return a single partitioned dataset based on
                            // static
                            // static partitions in the relation
                            return datasetFactory.getDataset(
                                p.toUri(), relation.schema(), datasetFacetsBuilder);
                          })
                      .collect(Collectors.toList());
                }
              })
          .orElse(Collections.emptyList());
    } catch (Exception e) {
      if ("com.databricks.backend.daemon.data.client.adl.AzureCredentialNotFoundExcepgittion"
          .equals(e.getClass().getName())) {
        // This is a fallback that can occur when hadoop configurations cannot be
        // reached. This occurs in Azure Databricks when credential passthrough
        // is enabled and you're attempting to get the data lake credentials.
        // The Spark Listener context cannot use the user credentials
        // thus we need a fallback.
        // This is similar to the InsertIntoHadoopRelationVisitor's process for getting
        // Datasets
        List<D> inputDatasets = new ArrayList<D>();
        List<Path> paths =
            new ArrayList<>(JavaConversions.asJavaCollection(relation.location().rootPaths()));
        for (Path p : paths) {
          inputDatasets.add(datasetFactory.getDataset(p.toUri(), relation.schema()));
        }
        if (inputDatasets.isEmpty()) {
          return Collections.emptyList();
        } else {
          return inputDatasets;
        }
      } else {
        throw e;
      }
    }
  }

  private boolean isSingleFileRelation(Collection<Path> paths, Configuration hadoopConfig) {
    if (paths.size() != 1) {
      return false;
    }

    try {
      Path path = paths.stream().findFirst().get();
      return path.getFileSystem(hadoopConfig).isFile(path);
      /*
       Unfortunately it seems like on DataBricks this can throw an SparkException as well if credentials are missing.
       Like org.apache.spark.SparkException: There is no Credential Scope.
      */
    } catch (Exception e) {
      return false;
    }
  }

  protected Optional<String> getDatasetVersion(LogicalRelation x) {
    // not implemented
    return Optional.empty();
  }
}
