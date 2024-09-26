package io.openlineage.spark.agent.vendor.redshift.lifecycle;

import io.github.spark_redshift_community.spark.redshift.Parameters;
import io.github.spark_redshift_community.spark.redshift.RedshiftRelation;
import io.github.spark_redshift_community.spark.redshift.TableName;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;

/**
 * {@link LogicalPlan} visitor that matches {@link SaveIntoDataSourceCommand}s that use a {@link
 * RedshiftRelation}. This function extracts a {@link OpenLineage.Dataset} from the Redshift table
 * referenced by the relation.
 */
@Slf4j
public class RedshiftRelationVisitor<D extends OpenLineage.Dataset>
    extends QueryPlanVisitor<LogicalRelation, D> {
  private static final String REDSHIFT_NAMESPACE = "redshift";
  private static final String REDSHIFT_CLASS_NAME =
      "io.github.spark_redshift_community.spark.redshift.RedshiftRelation";
  private final DatasetFactory<D> factory;

  public RedshiftRelationVisitor(@NonNull OpenLineageContext context, DatasetFactory<D> factory) {
    super(context);
    this.factory = factory;
    log.info("RedshiftRelationVisitor created");
  }

  @Override
  public List<D> apply(LogicalPlan x) {
    RedshiftRelation relation = (RedshiftRelation) ((LogicalRelation) x).relation();
    Parameters.MergedParameters params = relation.params();
    Optional<String> dbtable =
        (Optional<String>)
            ScalaConversionUtils.asJavaOptional(params.table().map(TableName::toString));
    Optional<String> query = ScalaConversionUtils.asJavaOptional(params.query());
    return Collections.singletonList(
        factory.getDataset(dbtable.orElse(""), REDSHIFT_NAMESPACE, relation.schema()));
  }

  protected boolean isRedshiftClass(LogicalPlan plan) {
    try {
      Class c = Thread.currentThread().getContextClassLoader().loadClass(REDSHIFT_CLASS_NAME);
      return (plan instanceof LogicalRelation
          && c.isAssignableFrom(((LogicalRelation) plan).relation().getClass()));
    } catch (Exception e) {
      // swallow - not a snowflake class
    }
    return false;
  }

  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    return isRedshiftClass(plan);
  }
}
