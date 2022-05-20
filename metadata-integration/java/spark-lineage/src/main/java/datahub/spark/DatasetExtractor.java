package datahub.spark;

import datahub.spark.model.LineageUtils;
import datahub.spark.model.dataset.CatalogTableDataset;
import datahub.spark.model.dataset.HdfsPathDataset;
import datahub.spark.model.dataset.JdbcDataset;
import datahub.spark.model.dataset.SparkDataset;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand;
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable;
import org.apache.spark.sql.sources.BaseRelation;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.FabricType;

import scala.Option;
import scala.collection.JavaConversions;

import com.typesafe.config.Config;

@Slf4j
public class DatasetExtractor {
  
  private static final Map<Class<? extends LogicalPlan>, PlanToDataset> PLAN_TO_DATASET = new HashMap<>();
  private static final Map<Class<? extends BaseRelation>, RelationToDataset> REL_TO_DATASET = new HashMap<>();
  private static final Set<Class<? extends LogicalPlan>> OUTPUT_CMD = ImmutableSet.of(
      InsertIntoHadoopFsRelationCommand.class, SaveIntoDataSourceCommand.class,
      CreateDataSourceTableAsSelectCommand.class, CreateHiveTableAsSelectCommand.class, InsertIntoHiveTable.class);
  private static final String DATASET_ENV_KEY = "metadata.dataset.env";
  private static final String DATASET_PLATFORM_INSTANCE_KEY = "metadata.dataset.platformInstance";
  // TODO InsertIntoHiveDirCommand, InsertIntoDataSourceDirCommand

 private DatasetExtractor() {
   
 }
  private static interface PlanToDataset {
    Optional<? extends SparkDataset> fromPlanNode(LogicalPlan plan, SparkContext ctx, Config datahubConfig);
  }

  private static interface RelationToDataset {
    Optional<? extends SparkDataset> fromRelation(BaseRelation rel, SparkContext ctx, Config datahubConfig);
  }

  static {
    PLAN_TO_DATASET.put(InsertIntoHadoopFsRelationCommand.class, (p, ctx, datahubConfig) -> {
      InsertIntoHadoopFsRelationCommand cmd = (InsertIntoHadoopFsRelationCommand) p;
      if (cmd.catalogTable().isDefined()) {
        return Optional.of(new CatalogTableDataset(cmd.catalogTable().get(), getCommonPlatformInstance(datahubConfig), getCommonFabricType(datahubConfig)));
      }
      return Optional.of(new HdfsPathDataset(cmd.outputPath(), getCommonPlatformInstance(datahubConfig), getCommonFabricType(datahubConfig)));
    });

    PLAN_TO_DATASET.put(LogicalRelation.class, (p, ctx, datahubConfig) -> {
      BaseRelation baseRel = ((LogicalRelation) p).relation();
      if (!REL_TO_DATASET.containsKey(baseRel.getClass())) {
        return Optional.empty();
      }
      return REL_TO_DATASET.get(baseRel.getClass()).fromRelation(baseRel, ctx, datahubConfig);
    });

    PLAN_TO_DATASET.put(SaveIntoDataSourceCommand.class, (p, ctx, datahubConfig) -> {

      SaveIntoDataSourceCommand cmd = (SaveIntoDataSourceCommand) p;

      Map<String, String> options = JavaConversions.mapAsJavaMap(cmd.options());
      String url = options.get("url"); // e.g. jdbc:postgresql://localhost:5432/sparktestdb
      if (!url.contains("jdbc")) {
        return Optional.empty();
      }

      String tbl = options.get("dbtable");
      return Optional.of(new JdbcDataset(url, tbl, getCommonPlatformInstance(datahubConfig), getCommonFabricType(datahubConfig)));
    });

    PLAN_TO_DATASET.put(CreateDataSourceTableAsSelectCommand.class, (p, ctx, datahubConfig) -> {
      CreateDataSourceTableAsSelectCommand cmd = (CreateDataSourceTableAsSelectCommand) p;
      // TODO what of cmd.mode()
      return Optional.of(new CatalogTableDataset(cmd.table(), getCommonPlatformInstance(datahubConfig), getCommonFabricType(datahubConfig)));
    });
    PLAN_TO_DATASET.put(CreateHiveTableAsSelectCommand.class, (p, ctx, datahubConfig) -> {
      CreateHiveTableAsSelectCommand cmd = (CreateHiveTableAsSelectCommand) p;
      return Optional.of(new CatalogTableDataset(cmd.tableDesc(), getCommonPlatformInstance(datahubConfig), getCommonFabricType(datahubConfig)));
    });
    PLAN_TO_DATASET.put(InsertIntoHiveTable.class, (p, ctx, datahubConfig) -> {
      InsertIntoHiveTable cmd = (InsertIntoHiveTable) p;
      return Optional.of(new CatalogTableDataset(cmd.table(), getCommonPlatformInstance(datahubConfig), getCommonFabricType(datahubConfig)));
    });

    PLAN_TO_DATASET.put(HiveTableRelation.class, (p, ctx, datahubConfig) -> {
      HiveTableRelation cmd = (HiveTableRelation) p;
      return Optional.of(new CatalogTableDataset(cmd.tableMeta(), getCommonPlatformInstance(datahubConfig), getCommonFabricType(datahubConfig)));
    });

    REL_TO_DATASET.put(HadoopFsRelation.class, (r, ctx, datahubConfig) -> {
      List<Path> res = JavaConversions.asJavaCollection(((HadoopFsRelation) r).location().rootPaths()).stream()
          .map(p -> getDirectoryPath(p, ctx.hadoopConfiguration())).distinct().collect(Collectors.toList());

      // TODO mapping to URN TBD
      return Optional.of(new HdfsPathDataset(res.get(0), getCommonPlatformInstance(datahubConfig), getCommonFabricType(datahubConfig)));
    });
    REL_TO_DATASET.put(JDBCRelation.class, (r, ctx, datahubConfig) -> {
      JDBCRelation rel = (JDBCRelation) r;
      Option<String> tbl = rel.jdbcOptions().parameters().get(JDBCOptions.JDBC_TABLE_NAME());
      if (tbl.isEmpty()) {
        return Optional.empty();
      }

      return Optional.of(new JdbcDataset(rel.jdbcOptions().url(), tbl.get(), getCommonPlatformInstance(datahubConfig), getCommonFabricType(datahubConfig)));
    });
  }

  static Optional<? extends SparkDataset> asDataset(LogicalPlan logicalPlan, SparkContext ctx, boolean outputNode) {
    if (!outputNode && OUTPUT_CMD.contains(logicalPlan.getClass())) {
      return Optional.empty();
    }

    if (!PLAN_TO_DATASET.containsKey(logicalPlan.getClass())) {
      return Optional.empty();
    }
    Config datahubconfig = LineageUtils.parseSparkConfig();
    return PLAN_TO_DATASET.get(logicalPlan.getClass()).fromPlanNode(logicalPlan, ctx, datahubconfig);
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
    String fabricTypeString = datahubConfig.hasPath(DATASET_ENV_KEY)
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
    return datahubConfig.hasPath(DATASET_PLATFORM_INSTANCE_KEY) ? datahubConfig.getString(DATASET_PLATFORM_INSTANCE_KEY)
        : null;
  }
}
