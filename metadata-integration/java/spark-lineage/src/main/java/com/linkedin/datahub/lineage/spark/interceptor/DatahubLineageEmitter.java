package com.linkedin.datahub.lineage.spark.interceptor;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
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

import com.google.common.base.Splitter;
import com.linkedin.datahub.lineage.spark.model.AppEndEvent;
import com.linkedin.datahub.lineage.spark.model.AppStartEvent;
import com.linkedin.datahub.lineage.spark.model.DatasetLineage;
import com.linkedin.datahub.lineage.spark.model.LineageConsumer;
import com.linkedin.datahub.lineage.spark.model.SQLQueryExecEndEvent;
import com.linkedin.datahub.lineage.spark.model.SQLQueryExecStartEvent;
import com.linkedin.datahub.lineage.spark.model.dataset.SparkDataset;

import lombok.extern.slf4j.Slf4j;
import scala.collection.JavaConversions;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractPartialFunction;

@Slf4j
public class DatahubLineageEmitter extends SparkListener {

  private static final int THREAD_CNT = 16;
  public static final String CONSUMER_TYPE_KEY = "spark.datahub.lineage.consumerTypes";

  private final Map<String, AppStartEvent> appDetails = new ConcurrentHashMap<>();
  private final Map<String, Map<Long, SQLQueryExecStartEvent>> appSqlDetails = new ConcurrentHashMap<>();
  private final Map<String, ExecutorService> appPoolDetails = new ConcurrentHashMap<>();

//  private static LineageConsumer loggingConsumer() {
//    log.warn("Lineage consumer not specified. Defaulting to LoggingConsumer.");
//    return LineageUtils.LOGGING_CONSUMER;
//  }

  private class SqlStartTask implements Runnable {

    private SparkListenerSQLExecutionStart sqlStart;
    private SparkContext ctx;
    private LogicalPlan plan;

    public SqlStartTask(SparkListenerSQLExecutionStart sqlStart, LogicalPlan plan, SparkContext ctx) {
      this.sqlStart = sqlStart;
      this.plan = plan;
      this.ctx = ctx;
    }

    @Override
    public void run() {
      appSqlDetails.get(ctx.appName()).put(sqlStart.executionId(),
          new SQLQueryExecStartEvent(ctx.conf().get("spark.master"), ctx.appName(), ctx.applicationId(),
              sqlStart.time(), sqlStart.executionId(), null));
      log.debug("PLAN for execution id: " + ctx.appName() + ":" + sqlStart.executionId() + "\n");
      log.debug(plan.toString());

      DatasetExtractor extractor = new DatasetExtractor();
      Optional<? extends SparkDataset> outputDS = extractor.asDataset(plan, ctx, true);
      if (!outputDS.isPresent()) {
        log.debug("Skipping execution as no output dataset present for execution id: " + ctx.appName() + ":"
            + sqlStart.executionId());
        return;
      }

      DatasetLineage lineage = new DatasetLineage(sqlStart.description(), plan.toString(), outputDS.get());
      Collection<QueryPlan<?>> allInners = new ArrayList<>();

      plan.collect(new AbstractPartialFunction<LogicalPlan, Void>() {

        @Override
        public Void apply(LogicalPlan plan) {
          log.debug("CHILD " + plan.getClass() + "\n" + plan + "\n-------------\n");
          Optional<? extends SparkDataset> inputDS = extractor.asDataset(plan, ctx, false);
          inputDS.ifPresent(x -> lineage.addSource(x));
          allInners.addAll(JavaConversions.asJavaCollection(plan.innerChildren()));
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

        nestedPlan.collect(new AbstractPartialFunction<LogicalPlan, Void>() {

          @Override
          public Void apply(LogicalPlan plan) {
            log.debug("INNER CHILD " + plan.getClass() + "\n" + plan + "\n-------------\n");
            Optional<? extends SparkDataset> inputDS = extractor.asDataset(plan, ctx, false);
            inputDS.ifPresent(
                x -> log.debug("source added for " + ctx.appName() + "/" + sqlStart.executionId() + ": " + x));
            inputDS.ifPresent(x -> lineage.addSource(x));
            return null;
          }

          @Override
          public boolean isDefinedAt(LogicalPlan x) {
            return true;
          }
        });
      }

      SQLQueryExecStartEvent evt = new SQLQueryExecStartEvent(ctx.conf().get("spark.master"), ctx.appName(),
          ctx.applicationId(),
          sqlStart.time(), sqlStart.executionId(), lineage);

      appSqlDetails.get(ctx.appName()).put(sqlStart.executionId(), evt);

      consumers().forEach(c -> c.accept(evt)); // TODO parallel stream here?

      log.debug("LINEAGE \n" + lineage + "\n");
      log.debug("Parsed execution id " + ctx.appName() + ":" + sqlStart.executionId());

      return;
    }

  }

  @Override
  public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
    try {
      log.debug("App started: " + applicationStart);
      LineageUtils.findSparkCtx().foreach(new AbstractFunction1<SparkContext, Void>() {

        @Override
        public Void apply(SparkContext sc) {
          String appId = applicationStart.appId().isDefined() ? applicationStart.appId().get() : "";
          AppStartEvent evt = new AppStartEvent(LineageUtils.getMaster(sc), applicationStart.appName(), appId,
              applicationStart.time(), applicationStart.sparkUser());

          consumers().forEach(x -> x.accept(evt));
          // TODO keyed by appName; only latest will be considered. Potential
          // inconsistencies not mapped.
          appDetails.put(applicationStart.appName(), evt);
          appSqlDetails.put(applicationStart.appName(), new ConcurrentHashMap<>());
          ExecutorService pool = Executors.newFixedThreadPool(THREAD_CNT);
          appPoolDetails.put(applicationStart.appName(), pool);
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
      LineageUtils.findSparkCtx().foreach(new AbstractFunction1<SparkContext, Void>() {

        @Override
        public Void apply(SparkContext sc) {
          log.debug("Application end event received for appId :" + sc.appName());
          AppStartEvent start = appDetails.remove(sc.appName());
          appPoolDetails.remove(sc.appName()).shutdown();
          appSqlDetails.remove(sc.appName());
          if (start == null) {
            log.error(
                "Application end event received, but start event missing for appId " + sc.applicationId());
          } else {
            AppEndEvent evt = new AppEndEvent(LineageUtils.getMaster(sc), sc.appName(), sc.applicationId(),
                applicationEnd.time(), start);

            consumers().forEach(x -> x.accept(evt));
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
    LineageUtils.findSparkCtx().foreach(new AbstractFunction1<SparkContext, Void>() {

      @Override
      public Void apply(SparkContext sc) {
        SQLQueryExecStartEvent start = appSqlDetails.get(sc.appName()).remove(sqlEnd.executionId());
        if (start == null) {
          log.error("Execution end event received, but start event missing for appId/sql exec Id " + sc.applicationId()
              + ":" + sqlEnd.executionId());
        } else if (start.getDatasetLineage() != null) {
//              JobStatus status = jobEnd.jobResult().equals(org.apache.spark.scheduler.JobSucceeded$.MODULE$)
//                  ? JobStatus.COMPLETED
//                  : JobStatus.FAILED;
          SQLQueryExecEndEvent evt = new SQLQueryExecEndEvent(LineageUtils.getMaster(sc), sc.appName(),
              sc.applicationId(),
              sqlEnd.time(), sqlEnd.executionId(), start);

          consumers().forEach(x -> x.accept(evt));
        }
        return null;
      }
    });
  }

  // TODO sqlEvt.details() unused
  private void processExecution(SparkListenerSQLExecutionStart sqlStart) {
    QueryExecution queryExec = SQLExecution.getQueryExecution(sqlStart.executionId());
    if (queryExec == null) {
      log.error("Skipping processing for sql exec Id" + sqlStart.executionId() + " as Query execution context could not be read from current spark state");
      return;
    }
    LogicalPlan plan = queryExec.optimizedPlan();
    SparkSession sess = queryExec.sparkSession();
    SparkContext ctx = sess.sparkContext();
    ExecutorService pool = appPoolDetails.get(ctx.appName());
    pool.execute(new SqlStartTask(sqlStart, plan, ctx));
  }

  private static List<LineageConsumer> consumers() {
    SparkConf conf = SparkEnv.get().conf();
    if (conf.contains(CONSUMER_TYPE_KEY)) {
      String consumerTypes = conf.get(CONSUMER_TYPE_KEY);

      return StreamSupport.stream(Splitter.on(",").trimResults().split(consumerTypes).spliterator(), false)
          .map(x -> LineageUtils.getConsumer(x)).filter(x -> x != null).collect(Collectors.toList());
    } else {
      return Collections.singletonList(LineageUtils.getConsumer("mcpEmitter"));
    }

  }

}
