package datahub.spark.conf;

import java.util.Map;
import lombok.Data;

@Data
public class SparkAppContext {
  String appName;
  String appId;
  Long startTime;
  String sparkUser;
  String appAttemptId;
  Map<String, String> databricksTags;
}
