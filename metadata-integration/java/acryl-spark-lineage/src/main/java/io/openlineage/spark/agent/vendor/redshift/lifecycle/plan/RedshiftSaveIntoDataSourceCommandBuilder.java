package io.openlineage.spark.agent.vendor.redshift.lifecycle.plan;

import static io.openlineage.spark.agent.vendor.redshift.RedshiftVendor.hasRedshiftClasses;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.agent.vendor.redshift.lifecycle.RedshiftDataset;
import io.openlineage.spark.api.AbstractQueryPlanDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.types.StructType;

@Slf4j
public class RedshiftSaveIntoDataSourceCommandBuilder
    extends AbstractQueryPlanDatasetBuilder<
        SparkListenerEvent, SaveIntoDataSourceCommand, OpenLineage.OutputDataset> {

  public RedshiftSaveIntoDataSourceCommandBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(SaveIntoDataSourceCommand command) {
    if (isRedshiftSource(command.dataSource())) {
      // Called from SaveIntoDataSourceCommandVisitor on Snowflake write operations.
      Map<String, String> options = ScalaConversionUtils.<String, String>fromMap(command.options());
      log.info("Redshift SaveIntoDataSourceCommand options: {}", options);
      Optional<String> dbtable = Optional.ofNullable(options.get("dbtable"));
      Optional<String> query = Optional.ofNullable(options.get("query"));
      String url = options.get("url");

      try {
        return
        // Similar to Kafka, Snowflake also has some special handling. So we use the method
        // below for extracting the dataset from Snowflake write operations.
        RedshiftDataset.getDatasets(
            outputDataset(), url, dbtable, query, getSchema(command)
            // command.schema() doesn't seem to contain the schema when tested with Azure
            // Snowflake,
            // so we use the helper to extract it from the logical plan.
            );
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    } else {
      return Collections.emptyList();
    }
  }

  public static boolean isRedshiftSource(CreatableRelationProvider provider) {
    return hasRedshiftClasses(); // && provider instanceof DefaultSource;
  }

  /**
   * Taken from {@link
   * io.openlineage.spark.agent.lifecycle.plan.SaveIntoDataSourceCommandVisitor#getSchema(org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand)}
   *
   * @param command
   * @return
   */
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
}
