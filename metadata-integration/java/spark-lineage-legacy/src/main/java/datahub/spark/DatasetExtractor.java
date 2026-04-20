package datahub.spark;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.FabricType;
import com.typesafe.config.Config;
import datahub.spark.model.LineageUtils;
import datahub.spark.model.dataset.CatalogTableDataset;
import datahub.spark.model.dataset.HdfsPathDataset;
import datahub.spark.model.dataset.JdbcDataset;
import datahub.spark.model.dataset.SparkDataset;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.FileSourceScanExec;
import org.apache.spark.sql.execution.RowDataSourceScanExec;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.columnar.InMemoryRelation;
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec;
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand;
import org.apache.spark.sql.hive.execution.HiveTableScanExec;
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable;
import org.apache.spark.sql.sources.BaseRelation;
import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.runtime.AbstractFunction1;

@Slf4j
public class DatasetExtractor {

  private static final Map<Class<? extends LogicalPlan>, PlanToDataset> PLAN_TO_DATASET =
      new HashMap<>();
  private static final Map<Class<? extends SparkPlan>, SparkPlanToDataset> SPARKPLAN_TO_DATASET =
      new HashMap<>();
  private static final Map<Class<? extends BaseRelation>, RelationToDataset> REL_TO_DATASET =
      new HashMap<>();
  private static final Set<Class<? extends LogicalPlan>> OUTPUT_CMD =
      ImmutableSet.of(
          InsertIntoHadoopFsRelationCommand.class,
          SaveIntoDataSourceCommand.class,
          CreateDataSourceTableAsSelectCommand.class,
          CreateHiveTableAsSelectCommand.class,
          InsertIntoHiveTable.class);
  private static final String DATASET_ENV_KEY = "metadata.dataset.env";
  private static final String DATASET_PLATFORM_INSTANCE_KEY = "metadata.dataset.platformInstance";
  private static final String TABLE_HIVE_PLATFORM_ALIAS = "metadata.table.hive_platform_alias";
  private static final String INCLUDE_SCHEME_KEY = "metadata.include_scheme";
  private static final String REMOVE_PARTITION_PATTERN = "metadata.remove_partition_pattern";

  // TODO InsertIntoHiveDirCommand, InsertIntoDataSourceDirCommand

  private DatasetExtractor() {}

  private static interface PlanToDataset {
    Optional<? extends Collection<SparkDataset>> fromPlanNode(
        LogicalPlan plan, SparkContext ctx, Config datahubConfig);
  }

  private static interface RelationToDataset {
    Optional<? extends Collection<SparkDataset>> fromRelation(
        BaseRelation rel, SparkContext ctx, Config datahubConfig);
  }

  private static interface SparkPlanToDataset {
    Optional<? extends Collection<SparkDataset>> fromSparkPlanNode(
        SparkPlan plan, SparkContext ctx, Config datahubConfig);
  }

  static {
    SPARKPLAN_TO_DATASET.put(
        FileSourceScanExec.class,
        (p, ctx, datahubConfig) -> {
          BaseRelation baseRel = ((FileSourceScanExec) p).relation();
          if (!REL_TO_DATASET.containsKey(baseRel.getClass())) {
            return Optional.empty();
          }
          return REL_TO_DATASET.get(baseRel.getClass()).fromRelation(baseRel, ctx, datahubConfig);
        });

    SPARKPLAN_TO_DATASET.put(
        HiveTableScanExec.class,
        (p, ctx, datahubConfig) -> {
          HiveTableRelation baseRel = ((HiveTableScanExec) p).relation();
          if (!PLAN_TO_DATASET.containsKey(baseRel.getClass())) {
            return Optional.empty();
          }
          return PLAN_TO_DATASET.get(baseRel.getClass()).fromPlanNode(baseRel, ctx, datahubConfig);
        });

    SPARKPLAN_TO_DATASET.put(
        RowDataSourceScanExec.class,
        (p, ctx, datahubConfig) -> {
          BaseRelation baseRel = ((RowDataSourceScanExec) p).relation();
          if (!REL_TO_DATASET.containsKey(baseRel.getClass())) {
            return Optional.empty();
          }
          return REL_TO_DATASET.get(baseRel.getClass()).fromRelation(baseRel, ctx, datahubConfig);
        });

    SPARKPLAN_TO_DATASET.put(
        InMemoryTableScanExec.class,
        (p, ctx, datahubConfig) -> {
          try {
            InMemoryRelation baseRel = ((InMemoryTableScanExec) p).relation();
            if (baseRel == null || !PLAN_TO_DATASET.containsKey(baseRel.getClass())) {
              return Optional.empty();
            }
            return PLAN_TO_DATASET
                .get(baseRel.getClass())
                .fromPlanNode(baseRel, ctx, datahubConfig);
          } catch (Exception e) {
            log.warn("Error extracting dataset from InMemoryTableScanExec: {}", e.getMessage());
            return Optional.empty();
          }
        });

    PLAN_TO_DATASET.put(
        InsertIntoHadoopFsRelationCommand.class,
        (p, ctx, datahubConfig) -> {
          InsertIntoHadoopFsRelationCommand cmd = (InsertIntoHadoopFsRelationCommand) p;
          if (cmd.catalogTable().isDefined()) {
            return Optional.of(
                Collections.singletonList(
                    new CatalogTableDataset(
                        cmd.catalogTable().get(),
                        getCommonPlatformInstance(datahubConfig),
                        getTableHivePlatformAlias(datahubConfig),
                        getCommonFabricType(datahubConfig))));
          }
          return Optional.of(
              Collections.singletonList(
                  new HdfsPathDataset(
                      cmd.outputPath(),
                      getCommonPlatformInstance(datahubConfig),
                      getIncludeScheme(datahubConfig),
                      getCommonFabricType(datahubConfig),
                      getRemovePartitionPattern(datahubConfig))));
        });

    PLAN_TO_DATASET.put(
        LogicalRelation.class,
        (p, ctx, datahubConfig) -> {
          BaseRelation baseRel = ((LogicalRelation) p).relation();
          if (!REL_TO_DATASET.containsKey(baseRel.getClass())) {
            return Optional.empty();
          }
          return REL_TO_DATASET.get(baseRel.getClass()).fromRelation(baseRel, ctx, datahubConfig);
        });

    PLAN_TO_DATASET.put(
        SaveIntoDataSourceCommand.class,
        (p, ctx, datahubConfig) -> {
          SaveIntoDataSourceCommand cmd = (SaveIntoDataSourceCommand) p;

          Map<String, String> options = JavaConversions.mapAsJavaMap(cmd.options());
          String url =
              options.getOrDefault("url", ""); // e.g. jdbc:postgresql://localhost:5432/sparktestdb
          if (url.contains("jdbc")) {
            String tbl = options.get("dbtable");
            return Optional.of(
                Collections.singletonList(
                    new JdbcDataset(
                        url,
                        tbl,
                        getCommonPlatformInstance(datahubConfig),
                        getCommonFabricType(datahubConfig))));
          } else if (options.containsKey("path")) {
            return Optional.of(
                Collections.singletonList(
                    new HdfsPathDataset(
                        new Path(options.get("path")),
                        getCommonPlatformInstance(datahubConfig),
                        getIncludeScheme(datahubConfig),
                        getCommonFabricType(datahubConfig),
                        getRemovePartitionPattern(datahubConfig))));
          } else {
            return Optional.empty();
          }
        });

    PLAN_TO_DATASET.put(
        CreateDataSourceTableAsSelectCommand.class,
        (p, ctx, datahubConfig) -> {
          CreateDataSourceTableAsSelectCommand cmd = (CreateDataSourceTableAsSelectCommand) p;
          // TODO what of cmd.mode()
          return Optional.of(
              Collections.singletonList(
                  new CatalogTableDataset(
                      cmd.table(),
                      getCommonPlatformInstance(datahubConfig),
                      getTableHivePlatformAlias(datahubConfig),
                      getCommonFabricType(datahubConfig))));
        });
    PLAN_TO_DATASET.put(
        CreateHiveTableAsSelectCommand.class,
        (p, ctx, datahubConfig) -> {
          CreateHiveTableAsSelectCommand cmd = (CreateHiveTableAsSelectCommand) p;
          return Optional.of(
              Collections.singletonList(
                  new CatalogTableDataset(
                      cmd.tableDesc(),
                      getCommonPlatformInstance(datahubConfig),
                      getTableHivePlatformAlias(datahubConfig),
                      getCommonFabricType(datahubConfig))));
        });
    PLAN_TO_DATASET.put(
        InsertIntoHiveTable.class,
        (p, ctx, datahubConfig) -> {
          InsertIntoHiveTable cmd = (InsertIntoHiveTable) p;
          return Optional.of(
              Collections.singletonList(
                  new CatalogTableDataset(
                      cmd.table(),
                      getCommonPlatformInstance(datahubConfig),
                      getTableHivePlatformAlias(datahubConfig),
                      getCommonFabricType(datahubConfig))));
        });

    PLAN_TO_DATASET.put(
        HiveTableRelation.class,
        (p, ctx, datahubConfig) -> {
          HiveTableRelation cmd = (HiveTableRelation) p;
          return Optional.of(
              Collections.singletonList(
                  new CatalogTableDataset(
                      cmd.tableMeta(),
                      getCommonPlatformInstance(datahubConfig),
                      getTableHivePlatformAlias(datahubConfig),
                      getCommonFabricType(datahubConfig))));
        });

    REL_TO_DATASET.put(
        HadoopFsRelation.class,
        (r, ctx, datahubConfig) -> {
          List<Path> res =
              JavaConversions.asJavaCollection(((HadoopFsRelation) r).location().rootPaths())
                  .stream()
                  .map(p -> getDirectoryPath(p, ctx.hadoopConfiguration()))
                  .distinct()
                  .collect(Collectors.toList());

          // TODO mapping to URN TBD
          return Optional.of(
              Collections.singletonList(
                  new HdfsPathDataset(
                      res.get(0),
                      getCommonPlatformInstance(datahubConfig),
                      getIncludeScheme(datahubConfig),
                      getCommonFabricType(datahubConfig),
                      getRemovePartitionPattern(datahubConfig))));
        });
    REL_TO_DATASET.put(
        JDBCRelation.class,
        (r, ctx, datahubConfig) -> {
          JDBCRelation rel = (JDBCRelation) r;
          Option<String> tbl = rel.jdbcOptions().parameters().get(JDBCOptions.JDBC_TABLE_NAME());
          if (tbl.isEmpty()) {
            return Optional.empty();
          }

          return Optional.of(
              Collections.singletonList(
                  new JdbcDataset(
                      rel.jdbcOptions().url(),
                      tbl.get(),
                      getCommonPlatformInstance(datahubConfig),
                      getCommonFabricType(datahubConfig))));
        });

    // GlobalLimit handler (Spark 3.5+) — register via reflection for Spark 3.3.x runtime
    // compatibility
    try {
      @SuppressWarnings("unchecked")
      Class<? extends LogicalPlan> globalLimitClass =
          (Class<? extends LogicalPlan>)
              Class.forName("org.apache.spark.sql.catalyst.plans.logical.GlobalLimit");
      PLAN_TO_DATASET.put(
          globalLimitClass,
          (plan, ctx, datahubConfig) -> {
            // Some Spark 3.5 query shapes surface GlobalLimit as a wrapper around the child plan
            // that contains the actual dataset information. Unwrap and recurse to extract lineage.
            try {
              Method childMethod = plan.getClass().getMethod("child");
              LogicalPlan child = (LogicalPlan) childMethod.invoke(plan);
              if (child == null) {
                log.debug("GlobalLimit.child() returned null, unable to extract lineage");
                return Optional.empty();
              }
              return asDataset(child, ctx, false);
            } catch (Exception e) {
              log.warn("Error unwrapping GlobalLimit: {}", e.getMessage());
              return Optional.empty();
            }
          });
    } catch (ClassNotFoundException e) {
      log.debug("GlobalLimit not available in this Spark version (expected for Spark < 3.5)");
    }

    PLAN_TO_DATASET.put(
        InMemoryRelation.class,
        (plan, ctx, datahubConfig) -> {
          try {
            InMemoryRelation inMemRel = (InMemoryRelation) plan;
            SparkPlan cachedPlan = inMemRel.cachedPlan();

            if (cachedPlan == null) {
              log.info("InMemoryRelation has null cachedPlan, unable to extract lineage");
              return Optional.empty();
            }

            // In Spark 3.5, AQE (Adaptive Query Execution) may wrap cachedPlan in
            // AdaptiveSparkPlanExec. When isFinalPlan=false, collectLeaves() on the wrapper
            // returns empty, so we must unwrap to access the underlying inputPlan that
            // contains the actual leaf operators (FileSourceScan, HiveTableScan, etc.)
            // needed for lineage extraction.
            SparkPlan effectivePlan = unwrapAdaptiveSparkPlan(cachedPlan);

            ArrayList<SparkDataset> datasets = new ArrayList<>();
            Seq<SparkPlan> leaves = effectivePlan.collectLeaves().toList();

            if (leaves.isEmpty()) {
              log.debug(
                  "InMemoryRelation.collectLeaves() returned empty. Plan type: {}",
                  effectivePlan.getClass().getSimpleName());
              return Optional.empty();
            }

            leaves.foreach(
                new AbstractFunction1<SparkPlan, Void>() {
                  @Override
                  public Void apply(SparkPlan leafPlan) {
                    if (SPARKPLAN_TO_DATASET.containsKey(leafPlan.getClass())) {
                      try {
                        Optional<? extends Collection<SparkDataset>> dataset =
                            SPARKPLAN_TO_DATASET
                                .get(leafPlan.getClass())
                                .fromSparkPlanNode(leafPlan, ctx, datahubConfig);
                        dataset.ifPresent(x -> datasets.addAll(x));
                      } catch (Exception e) {
                        log.error(
                            "Error extracting dataset from InMemoryRelation leaf plan ({}): {}",
                            leafPlan.getClass().getSimpleName(),
                            e.getMessage());
                      }
                    } else {
                      log.debug(
                          "InMemoryRelation leaf plan type {} is not yet supported",
                          leafPlan.getClass().getSimpleName());
                    }
                    return null;
                  }
                });
            return datasets.isEmpty() ? Optional.empty() : Optional.of(datasets);
          } catch (Exception e) {
            log.error("Error processing InMemoryRelation: {}", e.getMessage(), e);
            return Optional.empty();
          }
        });
  }

  /**
   * Unwrap AdaptiveSparkPlanExec to get the underlying inputPlan. In Spark 3.5, AQE/cached-plan
   * paths can surface AdaptiveSparkPlanExec wrappers; when isFinalPlan=false, collectLeaves() on
   * the wrapper returns empty. We extract the underlying inputPlan to access the actual leaf
   * operators (FileSourceScan, HiveTableScan, etc.) needed for lineage extraction.
   *
   * <p>Uses reflection to avoid compile-time dependency on AdaptiveSparkPlanExec, which may not
   * exist in older Spark versions.
   */
  private static SparkPlan unwrapAdaptiveSparkPlan(SparkPlan plan) {
    // Use exact class name match instead of contains() to avoid false positives
    // AdaptiveSparkPlanExec only exists in Spark 3.3+; defensive check handles older versions
    if (plan.getClass()
        .getName()
        .equals("org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec")) {
      try {
        Method inputPlanMethod = plan.getClass().getMethod("inputPlan");
        SparkPlan inputPlan = (SparkPlan) inputPlanMethod.invoke(plan);
        log.debug("Unwrapped AdaptiveSparkPlanExec to {}", inputPlan.getClass().getName());
        return inputPlan;
      } catch (NoSuchMethodException e) {
        // Method doesn't exist in this Spark version; return plan unchanged
        log.debug("AdaptiveSparkPlanExec.inputPlan() not available, using original plan");
      } catch (InvocationTargetException | IllegalAccessException e) {
        // Invocation failed; return plan unchanged
        log.debug("Failed to invoke AdaptiveSparkPlanExec.inputPlan(), using original plan");
      }
    }
    return plan;
  }

  /**
   * Check if a LogicalPlan is a create-table-as-select command
   * (CreateDataSourceTableAsSelectCommand or CreateHiveTableAsSelectCommand).
   */
  static boolean isCreateTableAsSelectCommand(LogicalPlan plan) {
    return plan instanceof CreateDataSourceTableAsSelectCommand
        || plan instanceof CreateHiveTableAsSelectCommand;
  }

  /**
   * Check if a LogicalPlan is an internal follow-up insert command that Spark 3.5+ generates after
   * a create-table-as-select. These should be skipped to avoid duplicate lineage entries.
   */
  static boolean isFollowUpInsertCommand(LogicalPlan plan) {
    return plan instanceof InsertIntoHadoopFsRelationCommand || plan instanceof InsertIntoHiveTable;
  }

  /**
   * Extract target table identifier from a logical plan (CTAS or follow-up insert). Returns
   * normalized table name (lowercase, without spark_catalog. prefix) or null if extraction fails.
   * For InsertIntoHadoopFsRelationCommand: only returns a table name if catalogTable is defined. If
   * catalogTable is not defined (file-based insert), returns null so follow-ups aren't matched.
   */
  static String getTargetTable(LogicalPlan plan) {
    if (plan instanceof CreateDataSourceTableAsSelectCommand) {
      return getTableIdentifier(((CreateDataSourceTableAsSelectCommand) plan).table());
    } else if (plan instanceof CreateHiveTableAsSelectCommand) {
      return getTableIdentifier(((CreateHiveTableAsSelectCommand) plan).tableDesc());
    } else if (plan instanceof InsertIntoHiveTable) {
      return getTableIdentifier(((InsertIntoHiveTable) plan).table());
    } else if (plan instanceof InsertIntoHadoopFsRelationCommand) {
      InsertIntoHadoopFsRelationCommand cmd = (InsertIntoHadoopFsRelationCommand) plan;
      if (cmd.catalogTable().isDefined()) {
        return getTableIdentifier(cmd.catalogTable().get());
      }
      // Extract table from warehouse path: /path/to/db.db/table → db.table
      String path = cmd.outputPath().toString();
      String[] parts = path.split("/");
      if (parts.length >= 2) {
        String dbPart = parts[parts.length - 2]; // e.g., "sparktestdb.db"
        String tablePart = parts[parts.length - 1]; // e.g., "foo4"
        if (dbPart.endsWith(".db") && !tablePart.isEmpty()) {
          String dbName = dbPart.substring(0, dbPart.length() - 3); // remove ".db"
          return (dbName + "." + tablePart).toLowerCase();
        }
      }
      return null;
    }
    return null;
  }

  /**
   * Get normalized table identifier from CatalogTable. Strips spark_catalog. prefix and lowercases
   * for case-insensitive matching. Used for semantic CTAS dedup.
   */
  private static String getTableIdentifier(CatalogTable table) {
    String qualifiedName = table.qualifiedName();
    if (qualifiedName.startsWith("spark_catalog.")) {
      qualifiedName = qualifiedName.substring("spark_catalog.".length());
    }
    return qualifiedName.toLowerCase();
  }

  static Optional<? extends Collection<SparkDataset>> asDataset(
      LogicalPlan logicalPlan, SparkContext ctx, boolean outputNode) {

    if (!outputNode && OUTPUT_CMD.contains(logicalPlan.getClass())) {
      return Optional.empty();
    }

    if (!PLAN_TO_DATASET.containsKey(logicalPlan.getClass())) {
      log.error(
          logicalPlan.getClass()
              + " is not supported yet. Please contact datahub team for further support. ");
      return Optional.empty();
    }
    Config datahubconfig = LineageUtils.parseSparkConfig();
    return PLAN_TO_DATASET
        .get(logicalPlan.getClass())
        .fromPlanNode(logicalPlan, ctx, datahubconfig);
  }

  private static Path getDirectoryPath(Path p, Configuration hadoopConf) {
    try {
      if (p.getFileSystem(hadoopConf).getFileStatus(p).isFile()) {
        return p.getParent();
      } else {
        return p;
      }
    } catch (IOException e) {
      return p;
    }
  }

  private static FabricType getCommonFabricType(Config datahubConfig) {
    String fabricTypeString =
        datahubConfig.hasPath(DATASET_ENV_KEY)
            ? datahubConfig.getString(DATASET_ENV_KEY).toUpperCase()
            : "PROD";
    FabricType fabricType = null;
    try {
      fabricType = FabricType.valueOf(fabricTypeString);
    } catch (IllegalArgumentException e) {
      log.warn("Invalid env ({}). Setting env to default PROD", fabricTypeString);
      fabricType = FabricType.PROD;
    }
    return fabricType;
  }

  private static String getCommonPlatformInstance(Config datahubConfig) {
    return datahubConfig.hasPath(DATASET_PLATFORM_INSTANCE_KEY)
        ? datahubConfig.getString(DATASET_PLATFORM_INSTANCE_KEY)
        : null;
  }

  private static String getTableHivePlatformAlias(Config datahubConfig) {
    return datahubConfig.hasPath(TABLE_HIVE_PLATFORM_ALIAS)
        ? datahubConfig.getString(TABLE_HIVE_PLATFORM_ALIAS)
        : "hive";
  }

  private static boolean getIncludeScheme(Config datahubConfig) {
    return datahubConfig.hasPath(INCLUDE_SCHEME_KEY)
        ? datahubConfig.getBoolean(INCLUDE_SCHEME_KEY)
        : true;
  }

  private static String getRemovePartitionPattern(Config datahubConfig) {
    return datahubConfig.hasPath(REMOVE_PARTITION_PATTERN)
        ? datahubConfig.getString(REMOVE_PARTITION_PATTERN)
        : null;
  }
}
