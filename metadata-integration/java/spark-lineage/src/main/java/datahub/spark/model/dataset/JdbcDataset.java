package datahub.spark.model.dataset;

import java.util.HashMap;
import java.util.Map;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DatasetUrn;

import datahub.spark.model.LineageUtils;
import io.opentracing.contrib.jdbc.parser.URLParser;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class JdbcDataset implements SparkDataset {
  //TODO: Should map to the central location on datahub for platform names
  private static final Map<String, String> PLATFORM_NAME_MAPPING = new HashMap<>();
  static {
    PLATFORM_NAME_MAPPING.put("postgresql", "postgres");
  }

  private final DatasetUrn urn;

  public JdbcDataset(String url, String tbl, String platformInstance, FabricType fabricType) {
    this.urn = LineageUtils.createDatasetUrn(platformName(url), platformInstance, dsName(url, tbl), fabricType);
  }

  @Override
  public DatasetUrn urn() {
    return this.urn;
  }

  private static String platformName(String url) {
    String dbType = URLParser.parse(url).getDbType();
    return PLATFORM_NAME_MAPPING.getOrDefault(dbType, dbType);
  }

  private static String dsName(String url, String tbl) {
    return URLParser.parse(url).getDbInstance() + "." + tbl;
  }
}
