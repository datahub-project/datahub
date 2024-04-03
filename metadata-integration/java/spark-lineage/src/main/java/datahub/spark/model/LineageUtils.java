package datahub.spark.model;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.TupleKey;
import com.linkedin.common.urn.Urn;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.apache.spark.SparkEnv;
import org.apache.spark.sql.SparkSession;
import scala.Option;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;

@Slf4j
public class LineageUtils {

  private static Map<String, LineageConsumer> consumers = new ConcurrentHashMap<>();

  // hook for replacing paths during testing. Not the cleanest way, TODO improve.
  /* This is for generating urn from a hash of the plan */
  // private static Function<String, String> PATH_REPLACER = (x -> x);

  private LineageUtils() {}

  public static Urn dataPlatformInstanceUrn(String platform, String instance)
      throws URISyntaxException {
    return new Urn(
        "dataPlatformInstance",
        new TupleKey(Arrays.asList(new DataPlatformUrn(platform).toString(), instance)));
  }

  public static DataFlowUrn flowUrn(String master, String appName) {
    return new DataFlowUrn(
        "spark", appName, master.replaceAll(":", "_").replaceAll("/", "_").replaceAll("[_]+", "_"));
  }

  public static Option<SparkContext> findSparkCtx() {
    return SparkSession.getActiveSession()
        .map(
            new AbstractFunction1<SparkSession, SparkContext>() {

              @Override
              public SparkContext apply(SparkSession sess) {
                return sess.sparkContext();
              }
            })
        .orElse(
            new AbstractFunction0<Option<SparkContext>>() {

              @Override
              public Option<SparkContext> apply() {
                return SparkContext$.MODULE$.getActive();
              }
            });
  }

  public static String getMaster(SparkContext ctx) {
    return ctx.conf().get("spark.master");
  }

  // overwrites existing consumer entry of same type
  public static void registerConsumer(String consumerType, LineageConsumer consumer) {
    consumers.put(consumerType, consumer);
  }

  public static LineageConsumer getConsumer(String consumerType) {
    return consumers.get(consumerType);
  }

  public static Config parseSparkConfig() {
    SparkConf conf = SparkEnv.get().conf();
    String propertiesString =
        Arrays.stream(conf.getAllWithPrefix("spark.datahub."))
            .map(tup -> tup._1 + "= \"" + tup._2 + "\"")
            .collect(Collectors.joining("\n"));
    return ConfigFactory.parseString(propertiesString);
  }

  // TODO: URN creation with platform instance needs to be inside DatasetUrn class
  public static DatasetUrn createDatasetUrn(
      String platform, String platformInstance, String name, FabricType fabricType) {
    String datasteName = platformInstance == null ? name : platformInstance + "." + name;
    return new DatasetUrn(new DataPlatformUrn(platform), datasteName, fabricType);
  }

  /* This is for generating urn from a hash of the plan */

  /*
   * public static String scrubPlan(String plan) { String s =
   * plan.replaceAll("#[0-9]*", ""); s =
   * s.replaceAll("JdbcRelationProvider@[0-9a-zA-Z]*,", "JdbcRelationProvider,");
   * s = s.replaceAll("InMemoryFileIndex@[0-9a-zA-Z]*,", "InMemoryFileIndex,"); s
   * = s.replaceAll("Created Time:[^\n]+\n", ""); s =
   * s.replaceAll("Last Access:[^\n]+\n", ""); s = s.replaceAll("Owner:[^\n]+\n",
   * ""); s = s.replaceAll("Statistics:[^\n]+\n", ""); s =
   * s.replaceAll("Table Properties:[^\n]+\n", ""); //
   * System.out.println("CLEAN: " + s); return s; }
   *
   * public static void setPathReplacer(Function<String, String> replacer) {
   * PATH_REPLACER = replacer; }
   *
   * public static String hash(String s) { s = PATH_REPLACER.apply(s);
   * log.debug("PATH REPLACED " + s); return Hashing.md5().hashString(s,
   * Charset.forName("US-ASCII")).toString(); }
   */
}
