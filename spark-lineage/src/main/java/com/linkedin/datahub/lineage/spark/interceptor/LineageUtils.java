package com.linkedin.datahub.lineage.spark.interceptor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;

import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.apache.spark.sql.SparkSession;

import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.JacksonDataTemplateCodec;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.lineage.consumer.impl.McpEmitter;
import com.linkedin.datahub.lineage.spark.model.LineageConsumer;
import com.linkedin.mxe.GenericAspect;

import lombok.extern.slf4j.Slf4j;
import scala.Option;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;

@Slf4j
public class LineageUtils {
  private static final JacksonDataTemplateCodec DATA_TEMPLATE_CODEC = new JacksonDataTemplateCodec();

  private static Map<String, LineageConsumer> consumers = new ConcurrentHashMap<>();

  public static final LineageConsumer LOGGING_CONSUMER = (x -> log.info(x.toString()));

  // hook for replacing paths during testing. Not the cleanest way, TODO improve.
  /* This is for generating urn from a hash of the plan */
  // private static Function<String, String> PATH_REPLACER = (x -> x);

  static {
    // system defined consumers
    registerConsumer("mcpEmitter", new McpEmitter());
  }

  private LineageUtils() {

  }

  // overwrites existing consumer entry of same type
  public static void registerConsumer(String consumerType, LineageConsumer consumer) {
    consumers.put(consumerType, consumer);
  }

  public static LineageConsumer getConsumer(String consumerType) {
    return consumers.get(consumerType);
  }

  public static DataFlowUrn flowUrn(String master, String appName) {
    return new DataFlowUrn("spark", appName, master.replaceAll(":", "_").replaceAll("/", "_").replaceAll("[_]+", "_"));
  }

  // Taken from GenericAspectUtils
  public static GenericAspect serializeAspect(@Nonnull RecordTemplate aspect) {
    GenericAspect genericAspect = new GenericAspect();

    try {
      String aspectStr = DATA_TEMPLATE_CODEC.mapToString(aspect.data());
      genericAspect.setValue(
          ByteString.unsafeWrap(aspectStr.getBytes(StandardCharsets.UTF_8)));
      genericAspect.setContentType("application/json");
      return genericAspect;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  public static Option<SparkContext> findSparkCtx() {
    return SparkSession.getActiveSession()
        .map(new AbstractFunction1<SparkSession, SparkContext>() {

          @Override
          public SparkContext apply(SparkSession sess) {
            return sess.sparkContext();
          }
        })
        .orElse(new AbstractFunction0<Option<SparkContext>>() {

          @Override
          public Option<SparkContext> apply() {
            return SparkContext$.MODULE$.getActive();
          }
        });
  }

  public static String getMaster(SparkContext ctx) {
    return ctx.conf().get("spark.master");
  }
  
  /* This is for generating urn from a hash of the plan */
  
/*
  public static String scrubPlan(String plan) {
    String s = plan.replaceAll("#[0-9]*", "");
    s = s.replaceAll("JdbcRelationProvider@[0-9a-zA-Z]*,", "JdbcRelationProvider,");
    s = s.replaceAll("InMemoryFileIndex@[0-9a-zA-Z]*,", "InMemoryFileIndex,");
    s = s.replaceAll("Created Time:[^\n]+\n", "");
    s = s.replaceAll("Last Access:[^\n]+\n", "");
    s = s.replaceAll("Owner:[^\n]+\n", "");
    s = s.replaceAll("Statistics:[^\n]+\n", "");
    s = s.replaceAll("Table Properties:[^\n]+\n", "");
    // System.out.println("CLEAN: " + s);
    return s;
  }

  public static void setPathReplacer(Function<String, String> replacer) {
    PATH_REPLACER = replacer;
  }
  
  public static String hash(String s) {
    s = PATH_REPLACER.apply(s);
    log.debug("PATH REPLACED " + s);
    return Hashing.md5().hashString(s, Charset.forName("US-ASCII")).toString();
  }
  */
}
