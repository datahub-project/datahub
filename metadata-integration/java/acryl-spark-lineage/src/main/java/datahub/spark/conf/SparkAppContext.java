package datahub.spark.conf;

import java.util.Map;
import lombok.Data;
import org.apache.spark.SparkConf;

@Data
public class SparkAppContext {
  String appName;
  String appId;
  Long startTime;
  String sparkUser;
  String appAttemptId;
  Map<String, String> databricksTags;
  private SparkConf sparkConf;

  /**
   * Returns the Spark configuration.
   *
   * @return the SparkConf object
   */
  public SparkConf getConf() {
    return sparkConf;
  }

  /**
   * Sets the Spark configuration.
   *
   * @param sparkConf the SparkConf to set
   */
  public void setConf(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
  }
}
