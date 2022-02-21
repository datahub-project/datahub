package datahub.spark.model.dataset;

import java.util.HashMap;
import java.util.Map;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;

import io.opentracing.contrib.jdbc.parser.URLParser;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class JdbcDataset implements SparkDataset {
  private static final Map<String, String> platformNameMapping = new HashMap<>();
  static {
    platformNameMapping.put("postgresql", "postgres");
  }

  private final DatasetUrn urn;

  public JdbcDataset(String url, String tbl, FabricType fabricType) {
    this.urn = new DatasetUrn(new DataPlatformUrn(platformName(url)), dsName(url, tbl), fabricType);
  }

  @Override
  public DatasetUrn urn() {
    return this.urn;
  }

  private static String platformName(String url) {
    String dbType = URLParser.parse(url).getDbType();
    return platformNameMapping.getOrDefault(dbType, dbType);
  }

  private static String dsName(String url, String tbl) {
    return URLParser.parse(url).getDbInstance() + "." + tbl;
  }
}
