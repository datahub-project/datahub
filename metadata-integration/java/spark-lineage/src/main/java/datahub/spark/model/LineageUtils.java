package datahub.spark.model;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.apache.spark.sql.SparkSession;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.TupleKey;
import com.linkedin.common.urn.Urn;

import scala.Option;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;

public class LineageUtils {

  private static Map<String, LineageConsumer> consumers = new ConcurrentHashMap<>();


  private LineageUtils() {

  }

  public static Urn dataPlatformInstanceUrn(String platform, String instance) throws URISyntaxException {
    return new Urn("dataPlatformInstance",
        new TupleKey(Arrays.asList(new DataPlatformUrn(platform).toString(), instance)));
  }

  public static DataFlowUrn flowUrn(String master, String appName) {
    return new DataFlowUrn("spark", appName, master.replaceAll(":", "_").replaceAll("/", "_").replaceAll("[_]+", "_"));
  }

  public static Option<SparkContext> findSparkCtx() {
    return SparkSession.getActiveSession().map(new AbstractFunction1<SparkSession, SparkContext>() {

      @Override
      public SparkContext apply(SparkSession sess) {
        return sess.sparkContext();
      }
    }).orElse(new AbstractFunction0<Option<SparkContext>>() {

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

  // TODO: URN creation with platform instance needs to be inside DatasetUrn class
  public static DatasetUrn createDatasetUrn(String platform, String platformInstance, String name,
      FabricType fabricType) {
    String datasteName = platformInstance == null ? name : platformInstance + "." + name;
    return new DatasetUrn(new DataPlatformUrn(platform), datasteName, fabricType);
  }

}
