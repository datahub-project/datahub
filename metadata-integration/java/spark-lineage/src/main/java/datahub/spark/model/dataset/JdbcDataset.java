package datahub.spark.model.dataset;

import java.util.HashMap;
import java.util.Map;

import com.linkedin.common.FabricType;

import io.opentracing.contrib.jdbc.parser.URLParser;
import lombok.ToString;

@ToString
public class JdbcDataset extends SparkDataset {
  //TODO: Should map to the central location on datahub for platform names
  private static final Map<String, String> PLATFORM_NAME_MAPPING = new HashMap<>();
  static {
    PLATFORM_NAME_MAPPING.put("postgresql", "postgres");
  }

  public JdbcDataset(String url, String tbl, String platformInstance, FabricType fabricType) {
    super(platformName(url), platformInstance, dsName(url, tbl), fabricType);
  }

  private static String platformName(String url) {
    String dbType = URLParser.parse(url).getDbType();
    return PLATFORM_NAME_MAPPING.getOrDefault(dbType, dbType);
  }

  private static String dsName(String url, String tbl) {
    return URLParser.parse(url).getDbInstance() + "." + tbl;
  }
}
