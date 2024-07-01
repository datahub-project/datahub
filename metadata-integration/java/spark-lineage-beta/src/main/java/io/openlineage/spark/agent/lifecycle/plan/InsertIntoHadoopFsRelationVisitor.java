/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import scala.Option;

/**
 * {@link LogicalPlan} visitor that matches an {@link InsertIntoHadoopFsRelationCommand} and
 * extracts the output {@link OpenLineage.Dataset} being written.
 */
public class InsertIntoHadoopFsRelationVisitor
    extends QueryPlanVisitor<InsertIntoHadoopFsRelationCommand, OpenLineage.OutputDataset> {

  public InsertIntoHadoopFsRelationVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    InsertIntoHadoopFsRelationCommand command = (InsertIntoHadoopFsRelationCommand) x;

    Option<CatalogTable> catalogTable = command.catalogTable();
    OpenLineage.OutputDataset outputDataset;

    if (catalogTable.isEmpty()) {
      DatasetIdentifier di = PathUtils.fromURI(command.outputPath().toUri(), "file");
      if (SaveMode.Overwrite == command.mode()) {
        outputDataset =
            outputDataset()
                .getDataset(
                    di,
                    command.query().schema(),
                    OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE);
      } else {
        outputDataset = outputDataset().getDataset(di, command.query().schema());
      }
      return Collections.singletonList(outputDataset);
    } else {
      if (SaveMode.Overwrite == command.mode()) {
        return Collections.singletonList(
            outputDataset()
                .getDataset(
                    PathUtils.fromCatalogTable(catalogTable.get()),
                    catalogTable.get().schema(),
                    OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE));
      } else {
        return Collections.singletonList(
            outputDataset()
                .getDataset(
                    PathUtils.fromCatalogTable(catalogTable.get()), catalogTable.get().schema()));
      }
    }
  }

  @Override
  public Optional<String> jobNameSuffix(InsertIntoHadoopFsRelationCommand command) {
    if (command.catalogTable().isEmpty()) {
      DatasetIdentifier di = PathUtils.fromURI(command.outputPath().toUri(), "file");
      return Optional.of(trimPath(di.getName()));
    }
    return Optional.of(
        trimPath(PathUtils.fromCatalogTable(command.catalogTable().get()).getName()));
  }
}
