package datahub.spark;

import com.google.common.base.Splitter;
import com.typesafe.config.Config;
import datahub.spark.consumer.impl.CoalesceJobsEmitter;
import datahub.spark.consumer.impl.McpEmitter;
import datahub.spark.model.AppEndEvent;
import datahub.spark.model.AppStartEvent;
import datahub.spark.model.DatasetLineage;
import datahub.spark.model.LineageConsumer;
import datahub.spark.model.LineageUtils;
import datahub.spark.model.SQLQueryExecEndEvent;
import datahub.spark.model.SQLQueryExecStartEvent;
import datahub.spark.model.dataset.SparkDataset;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.package$;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.QueryPlan;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.apache.spark.util.JsonProtocol;
import scala.collection.JavaConversions;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractPartialFunction;

@Slf4j
public class DatahubSparkListener extends SparkListener {

  public static final String CONSUMER_TYPE_KEY = "spark.datahub.lineage.consumerTypes";
  public static final String DATAHUB_EMITTER = "mcpEmitter";
  public static final String DATABRICKS_CLUSTER_KEY = "databricks.cluster";
  public static final String PIPELINE_KEY = "metadata.pipeline";
  public static final String PIPELINE_PLATFORM_INSTANCE_KEY = PIPELINE_KEY + ".platformInstance";

  public static final String COALESCE_KEY = "coalesce_jobs";

  private final Map<String, AppStartEvent> appDetails = new ConcurrentHashMap<>();
  private final Map<String, Map<Long, SQLQueryExecStartEvent>> appSqlDetails =
      new ConcurrentHashMap<>();
  private final Map<String, McpEmitter> appEmitters = new ConcurrentHashMap<>();
  private final Map<String, Config> appConfig = new ConcurrentHashMap<>();
  private final Map<String, Set<String>> pendingCtasTables = new ConcurrentHashMap<>();
  // Cached Spark version components (parsed once at first access)
  private static volatile int sparkMajorVersion = -1;
  private static volatile int sparkMinorVersion = -1;

  public DatahubSparkListener() {
    log.info("DatahubSparkListener initialised.");
  }

  /** Parse and cache Spark version components once. Thread-safe via synchronized. */
  private static synchronized void ensureSparkVersionParsed() {
    if (sparkMinorVersion < 0) {
      try {
        String version = package$.MODULE$.SPARK_VERSION();
        String[] parts = version.split("\\.");
        sparkMajorVersion = parts.length > 0 ? Integer.parseInt(parts[0]) : 3;
        sparkMinorVersion = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
      } catch (Exception e) {
        log.warn("Failed to parse Spark version, defaulting to 3.x logic", e);
        sparkMajorVersion = 3;
        sparkMinorVersion = 3;
      }
    }
  }

  private int getSparkMinorVersion() {
    ensureSparkVersionParsed();
    return sparkMinorVersion;
  }

  /**
   * Serialize SparkListenerSQLExecutionStart to JSON string. Handles both Spark 3.4+ (Jackson API)
   * and older versions (json4s API). Uses reflection for version-specific APIs since we compile
   * against Spark 3.5.0 but need to support runtime versions up to 3.5+.
   *
   * <p>NOTE: Spark < 3.4 fallback returns Scala case class toString() representation, not JSON.
   * This produces a human-readable but non-JSON output. For JSON serialization on all Spark
   * versions, consider using json4s library. Current approach prioritizes compatibility with older
   * Spark versions where Jackson API is unavailable.
   */
  private static String serializeSqlStartEvent(SparkListenerSQLExecutionStart event) {
    try {
      // Use cached version from ensureSparkVersionParsed() to avoid re-parsing on every call
      ensureSparkVersionParsed();

      // Spark 3.4+: Use Jackson API (returns valid JSON)
      if (sparkMajorVersion > 3 || (sparkMajorVersion == 3 && sparkMinorVersion >= 4)) {
        Method method =
            JsonProtocol.class.getMethod("sparkEventToJsonString", SparkListenerEvent.class);
        return (String) method.invoke(null, event);
      } else {
        // Spark < 3.4: Try json4s-based sparkEventToJson via reflection
        try {
          Method sparkEventToJsonMethod =
              JsonProtocol.class.getMethod("sparkEventToJson", SparkListenerEvent.class);
          Object jsonValue = sparkEventToJsonMethod.invoke(null, event);
          // Use json4s.jackson.JsonMethods.compact() to produce valid JSON (not toString())
          Class<?> jsonMethodsClass = Class.forName("org.json4s.jackson.JsonMethods$");
          Object module = jsonMethodsClass.getField("MODULE$").get(null);
          Method compactMethod = jsonMethodsClass.getMethod("compact", Object.class);
          return (String) compactMethod.invoke(module, jsonValue);
        } catch (Exception e) {
          // Fall back to toString() if sparkEventToJson unavailable
          log.debug(
              "Spark {}.{} < 3.4: sparkEventToJson unavailable ({}), using toString()",
              sparkMajorVersion,
              sparkMinorVersion,
              e.getMessage());
          return event.toString();
        }
      }
    } catch (Exception e) {
      log.warn("Failed to serialize SQL execution event to JSON, using toString() fallback", e);
      return event.toString();
    }
  }

  private class SqlStartTask {

    private final SparkListenerSQLExecutionStart sqlStart;
    private final SparkContext ctx;
    private final LogicalPlan plan;

    public SqlStartTask(
        SparkListenerSQLExecutionStart sqlStart, LogicalPlan plan, SparkContext ctx) {
      this.sqlStart = sqlStart;
      this.plan = plan;
      this.ctx = ctx;

      String jsonPlan;
      try {
        jsonPlan = (plan != null) ? plan.toJSON() : null;
      } catch (Exception e) {
        jsonPlan = (plan != null) ? plan.nodeName() : null;
      }
      String sqlStartJson = (sqlStart != null) ? serializeSqlStartEvent(sqlStart) : null;
      log.debug(
          "SqlStartTask with parameters: sqlStart: {}, plan: {}, ctx: {}",
          sqlStartJson,
          jsonPlan,
          ctx);
    }

    public void run() {
      if (ctx == null) {
        log.error("Context is null skipping run");
        return;
      }

      if (ctx.conf() == null) {
        log.error("Context does not have config. Skipping run");
        return;
      }

      if (sqlStart == null) {
        log.error("sqlStart is null skipping run");
        return;
      }

      appSqlDetails
          .get(ctx.applicationId())
          .put(
              sqlStart.executionId(),
              new SQLQueryExecStartEvent(
                  ctx.conf().get("spark.master"),
                  getPipelineName(ctx),
                  ctx.applicationId(),
                  sqlStart.time(),
                  sqlStart.executionId(),
                  null));
      log.debug(
          "PLAN for execution id: " + getPipelineName(ctx) + ":" + sqlStart.executionId() + "\n");
      try {
        log.debug(plan.toString());
      } catch (Exception e) {
        log.debug("Plan toString failed for {}: {}", plan.nodeName(), e.getMessage());
      }

      Optional<? extends Collection<SparkDataset>> outputDS =
          DatasetExtractor.asDataset(plan, ctx, true);
      if (!outputDS.isPresent() || outputDS.get().isEmpty()) {
        log.debug(
            "Skipping execution as no output dataset present for execution id: "
                + ctx.applicationId()
                + ":"
                + sqlStart.executionId());
        return;
      }

      SparkDataset sinkDataset = outputDS.get().iterator().next();

      // Spark 3.5+ generates follow-up inserts for CTAS commands as separate SQL executions.
      // Spark < 3.5 does not, so no dedup needed.
      // Version check handles Spark 3.5.x, 4.0.x, 5.x, etc. (any version >= 3.5)
      if (sparkMajorVersion > 3 || (sparkMajorVersion == 3 && sparkMinorVersion >= 5)) {
        String appId = ctx.applicationId();

        // Check if this is a follow-up INSERT for a recently created table via CTAS.
        // Match by table name: if we just created this table via CTAS, suppress the follow-up.
        if (DatasetExtractor.isFollowUpInsertCommand(plan)) {
          String targetTable = DatasetExtractor.getTargetTable(plan);
          if (targetTable != null) {
            Set<String> pending = pendingCtasTables.get(appId);
            if (pending != null && pending.remove(targetTable)) {
              log.debug(
                  "Suppressing CTAS follow-up insert for table {} (app={}, execId={})",
                  targetTable,
                  appId,
                  sqlStart.executionId());
              if (pending.isEmpty()) {
                pendingCtasTables.remove(appId);
              }
              return;
            }
          }
        }

        // Track CTAS commands by target table name.
        // Edge case: if user does CTAS then immediate INSERT to same table, INSERT would be
        // suppressed. This is astronomically unlikely in practice and arguably impossible in
        // same execution thread.
        if (DatasetExtractor.isCreateTableAsSelectCommand(plan)) {
          String targetTable = DatasetExtractor.getTargetTable(plan);
          if (targetTable != null) {
            pendingCtasTables
                .computeIfAbsent(appId, k -> ConcurrentHashMap.newKeySet())
                .add(targetTable);
          }
        }
      }

      // Here assumption is that there will be only single target for single sql query
      String planString;
      try {
        planString = plan.toString();
      } catch (Exception e) {
        // In Spark 3.5+, toString() can fail on plans containing InMemoryRelation
        // with AdaptiveSparkPlanExec. Fall back to the class name.
        planString = plan.nodeName();
      }
      DatasetLineage lineage = new DatasetLineage(sqlStart.description(), planString, sinkDataset);
      Collection<QueryPlan<?>> allInners = new ArrayList<>();

      plan.collect(
          new AbstractPartialFunction<LogicalPlan, Void>() {

            @Override
            public Void apply(LogicalPlan plan) {
              log.debug("CHILD {}", plan.getClass());
              Optional<? extends Collection<SparkDataset>> inputDS =
                  DatasetExtractor.asDataset(plan, ctx, false);
              inputDS.ifPresent(x -> x.forEach(y -> lineage.addSource(y)));
              try {
                allInners.addAll(JavaConversions.asJavaCollection(plan.innerChildren()));
              } catch (Exception e) {
                // In Spark 3.5+, InMemoryRelation.innerChildren() can throw when
                // the cachedPlan is AdaptiveSparkPlanExec. The datasets are already
                // extracted via the PLAN_TO_DATASET handler above, so this is safe
                // to skip.
                log.debug(
                    "Failed to get innerChildren for {}: {}",
                    plan.getClass().getSimpleName(),
                    e.getMessage());
              }
              return null;
            }

            @Override
            public boolean isDefinedAt(LogicalPlan x) {
              return true;
            }
          });

      for (QueryPlan<?> qp : allInners) {
        if (!(qp instanceof LogicalPlan)) {
          continue;
        }
        LogicalPlan nestedPlan = (LogicalPlan) qp;

        nestedPlan.collect(
            new AbstractPartialFunction<LogicalPlan, Void>() {

              @Override
              public Void apply(LogicalPlan plan) {
                log.debug("INNER CHILD {}", plan.getClass());
                Optional<? extends Collection<SparkDataset>> inputDS =
                    DatasetExtractor.asDataset(plan, ctx, false);
                inputDS.ifPresent(
                    x ->
                        log.debug(
                            "source added for "
                                + ctx.appName()
                                + "/"
                                + sqlStart.executionId()
                                + ": "
                                + x));
                inputDS.ifPresent(x -> x.forEach(y -> lineage.addSource(y)));
                return null;
              }

              @Override
              public boolean isDefinedAt(LogicalPlan x) {
                return true;
              }
            });
      }

      SQLQueryExecStartEvent evt =
          new SQLQueryExecStartEvent(
              ctx.conf().get("spark.master"),
              getPipelineName(ctx),
              ctx.applicationId(),
              sqlStart.time(),
              sqlStart.executionId(),
              lineage);

      appSqlDetails.get(ctx.applicationId()).put(sqlStart.executionId(), evt);

      McpEmitter emitter = appEmitters.get(ctx.applicationId());
      if (emitter != null) {
        emitter.accept(evt);
      }
      consumers().forEach(c -> c.accept(evt));

      log.debug("LINEAGE \n{}\n", lineage);
      log.debug("Parsed execution id {}:{}", ctx.appName(), sqlStart.executionId());
    }
  }

  @Override
  public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
    try {
      log.info("Application started: " + applicationStart);
      LineageUtils.findSparkCtx()
          .foreach(
              new AbstractFunction1<SparkContext, Void>() {

                @Override
                public Void apply(SparkContext sc) {
                  checkOrCreateApplicationSetup(sc);
                  return null;
                }
              });
      super.onApplicationStart(applicationStart);
    } catch (Exception e) {
      // log error, but don't impact thread
      StringWriter s = new StringWriter();
      PrintWriter p = new PrintWriter(s);
      e.printStackTrace(p);
      log.error(s.toString());
      p.close();
    }
  }

  @Override
  public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
    try {
      LineageUtils.findSparkCtx()
          .foreach(
              new AbstractFunction1<SparkContext, Void>() {

                @Override
                public Void apply(SparkContext sc) {
                  log.info("Application ended : {} {}", sc.appName(), sc.applicationId());
                  AppStartEvent start = appDetails.remove(sc.applicationId());
                  appSqlDetails.remove(sc.applicationId());
                  pendingCtasTables.remove(sc.applicationId());
                  if (start == null) {
                    log.error(
                        "Application end event received, but start event missing for appId "
                            + sc.applicationId());
                  } else {
                    AppEndEvent evt =
                        new AppEndEvent(
                            LineageUtils.getMaster(sc),
                            getPipelineName(sc),
                            sc.applicationId(),
                            applicationEnd.time(),
                            start);

                    McpEmitter emitter = appEmitters.get(sc.applicationId());
                    if (emitter != null) {
                      emitter.accept(evt);
                      try {
                        emitter.close();
                        appEmitters.remove(sc.applicationId());
                      } catch (Exception e) {
                        log.warn("Failed to close underlying emitter due to {}", e.getMessage());
                      }
                    }
                    consumers()
                        .forEach(
                            x -> {
                              x.accept(evt);
                              try {
                                x.close();
                              } catch (IOException e) {
                                log.warn("Failed to close lineage consumer", e);
                              }
                            });
                  }
                  return null;
                }
              });
      super.onApplicationEnd(applicationEnd);
    } catch (Exception e) {
      // log error, but don't impact thread
      StringWriter s = new StringWriter();
      PrintWriter p = new PrintWriter(s);
      e.printStackTrace(p);
      log.error(s.toString());
      p.close();
    }
  }

  @Override
  public void onOtherEvent(SparkListenerEvent event) {
    try {
      if (event instanceof SparkListenerSQLExecutionStart) {
        SparkListenerSQLExecutionStart sqlEvt = (SparkListenerSQLExecutionStart) event;
        log.debug("SQL Exec start event with id " + sqlEvt.executionId());
        processExecution(sqlEvt);
      } else if (event instanceof SparkListenerSQLExecutionEnd) {
        SparkListenerSQLExecutionEnd sqlEvt = (SparkListenerSQLExecutionEnd) event;
        log.debug("SQL Exec end event with id " + sqlEvt.executionId());
        processExecutionEnd(sqlEvt);
      }
    } catch (Exception e) {
      // log error, but don't impact thread
      StringWriter s = new StringWriter();
      PrintWriter p = new PrintWriter(s);
      e.printStackTrace(p);
      log.error(s.toString());
      p.close();
    }
  }

  public void processExecutionEnd(SparkListenerSQLExecutionEnd sqlEnd) {
    LineageUtils.findSparkCtx()
        .foreach(
            new AbstractFunction1<SparkContext, Void>() {

              @Override
              public Void apply(SparkContext sc) {
                SQLQueryExecStartEvent start =
                    appSqlDetails.get(sc.applicationId()).remove(sqlEnd.executionId());
                if (start == null) {
                  log.error(
                      "Execution end event received, but start event missing for appId/sql exec Id "
                          + sc.applicationId()
                          + ":"
                          + sqlEnd.executionId());
                } else if (start.getDatasetLineage() != null) {
                  SQLQueryExecEndEvent evt =
                      new SQLQueryExecEndEvent(
                          LineageUtils.getMaster(sc),
                          sc.appName(),
                          sc.applicationId(),
                          sqlEnd.time(),
                          sqlEnd.executionId(),
                          start);
                  McpEmitter emitter = appEmitters.get(sc.applicationId());
                  if (emitter != null) {
                    emitter.accept(evt);
                  }
                }
                return null;
              }
            });
  }

  private synchronized void checkOrCreateApplicationSetup(SparkContext ctx) {
    ExecutorService pool = null;
    String appId = ctx.applicationId();
    Config datahubConfig = appConfig.get(appId);
    if (datahubConfig == null) {
      Config datahubConf = LineageUtils.parseSparkConfig();
      appConfig.put(appId, datahubConf);
      Config pipelineConfig =
          datahubConf.hasPath(PIPELINE_KEY)
              ? datahubConf.getConfig(PIPELINE_KEY)
              : com.typesafe.config.ConfigFactory.empty();
      AppStartEvent evt =
          new AppStartEvent(
              LineageUtils.getMaster(ctx),
              getPipelineName(ctx),
              appId,
              ctx.startTime(),
              ctx.sparkUser(),
              pipelineConfig);

      appEmitters
          .computeIfAbsent(
              appId,
              s ->
                  datahubConf.hasPath(COALESCE_KEY) && datahubConf.getBoolean(COALESCE_KEY)
                      ? new CoalesceJobsEmitter(datahubConf)
                      : new McpEmitter(datahubConf))
          .accept(evt);
      consumers().forEach(c -> c.accept(evt));
      appDetails.put(appId, evt);
      appSqlDetails.put(appId, new ConcurrentHashMap<>());
    }
  }

  private String getPipelineName(SparkContext cx) {
    Config datahubConfig =
        appConfig.computeIfAbsent(cx.applicationId(), s -> LineageUtils.parseSparkConfig());
    String name = "";
    if (datahubConfig.hasPath(DATABRICKS_CLUSTER_KEY)) {
      name = datahubConfig.getString(DATABRICKS_CLUSTER_KEY) + "_" + cx.applicationId();
    }
    name = cx.appName();
    // TODO: appending of platform instance needs to be done at central location
    // like adding constructor to dataflowurl
    if (datahubConfig.hasPath(PIPELINE_PLATFORM_INSTANCE_KEY)) {
      name = datahubConfig.getString(PIPELINE_PLATFORM_INSTANCE_KEY) + "." + name;
    }
    return name;
  }

  private void processExecution(SparkListenerSQLExecutionStart sqlStart) {
    QueryExecution queryExec = SQLExecution.getQueryExecution(sqlStart.executionId());
    if (queryExec == null) {
      log.error(
          "Skipping processing for sql exec Id"
              + sqlStart.executionId()
              + " as Query execution context could not be read from current spark state");
      return;
    }
    LogicalPlan plan = queryExec.optimizedPlan();
    SparkSession sess = queryExec.sparkSession();
    SparkContext ctx = sess.sparkContext();
    checkOrCreateApplicationSetup(ctx);
    (new SqlStartTask(sqlStart, plan, ctx)).run();
  }

  private List<LineageConsumer> consumers() {
    SparkConf conf = SparkEnv.get().conf();
    if (conf.contains(CONSUMER_TYPE_KEY)) {
      String consumerTypes = conf.get(CONSUMER_TYPE_KEY);
      return StreamSupport.stream(
              Splitter.on(",").trimResults().split(consumerTypes).spliterator(), false)
          .map(x -> LineageUtils.getConsumer(x))
          .filter(Objects::nonNull)
          .collect(Collectors.toList());
    } else {
      return Collections.emptyList();
    }
  }
}
