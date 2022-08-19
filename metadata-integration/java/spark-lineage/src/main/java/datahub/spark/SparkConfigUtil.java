package datahub.spark;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;

import com.linkedin.common.FabricType;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SparkConfigUtil {

  private static final String DATASET_KEY = "metadata.dataset";
  private static final String DATASET_PLATFORM_INSTANCE_KEY = "metadata.dataset.platformInstance";

  private static final String PLATFORM_KEY = "platform";

  private static final String PATH_SPEC_LIST_KEY = "path_spec_list";
  private static final String PLATFORM_INSTANCE_KEY = "platformInstance";
  private static final String FABRIC_TYPE_KEY = "env";
  private static final String PATH_ALIAS_LIST_KEY = ".path_alias_list";

  private SparkConfigUtil() {

  }

  public static Config parseSparkConfig() {
    SparkConf conf = SparkEnv.get().conf();
    String propertiesString = Arrays.stream(conf.getAllWithPrefix("spark.datahub."))
        .map(tup -> tup._1 + "= \"" + tup._2 + "\"").collect(Collectors.joining("\n"));
    return ConfigFactory.parseString(propertiesString);
  }

  public static String getCommonPlatformInstance() {
    Config datahubConfig = parseSparkConfig();
    return datahubConfig.hasPath(DATASET_PLATFORM_INSTANCE_KEY) ? datahubConfig.getString(DATASET_PLATFORM_INSTANCE_KEY)
        : null;
  }

  public static Config getDatasetConfig() {
    Config datahubConfig = parseSparkConfig();
    return datahubConfig.hasPath(DATASET_KEY) ? datahubConfig.getConfig(DATASET_KEY) : null;
  }

  public static FabricType getCommonFabricType() {

    return getFabricType(getDatasetConfig());
  }

  public static List<String> getPathSpecListForPlatform(String platform) {
    Config datahubConfig = parseSparkConfig();
    String key = PLATFORM_KEY + "." + platform + PATH_SPEC_LIST_KEY;
    if (datahubConfig.hasPath(key)) {
      log.debug(key + ":" + datahubConfig.getString(key));
      return Arrays.asList(datahubConfig.getString(key).split(","));
    }

    return null;
  }

  public static List<String> getPathAliasListForPlatform(String platform) {
    Config datahubConfig = parseSparkConfig();
    String aliasKey = PLATFORM_KEY + "." + platform + PATH_ALIAS_LIST_KEY;
    return datahubConfig.hasPath(aliasKey) ? Arrays.asList(datahubConfig.getString(aliasKey)
        .split(","))
        : Arrays.asList();
  }

  public static Config getPathAliasDetails(String pathAlias, String platform) {
    String pathAliasKey = PLATFORM_KEY + "." + platform + "." + pathAlias;
    Config datahubConfig = parseSparkConfig();
    return datahubConfig.hasPath(pathAliasKey) ? datahubConfig.getConfig(pathAliasKey) : null;
  }

  public static List<String> getPathSpecList(Config pathSpecConfig) {
    return pathSpecConfig.hasPath(PATH_SPEC_LIST_KEY) ? Arrays.asList(pathSpecConfig.getString(PATH_SPEC_LIST_KEY)
        .split(",")) : null;
  }

  public static String getPlatformInstance(Config pathSpecConfig) {
    return pathSpecConfig.hasPath(PATH_SPEC_LIST_KEY) ? pathSpecConfig.getString(PLATFORM_INSTANCE_KEY)
        : null;
  }

  public static FabricType getFabricType(Config pathSpecConfig) {
    // setting default fabric type as 'PROD'
    FabricType fabricType = FabricType.PROD;
    if (pathSpecConfig != null) {
      String fabricTypeString = pathSpecConfig.hasPath(FABRIC_TYPE_KEY)
          ? pathSpecConfig.getString(FABRIC_TYPE_KEY).toUpperCase()
          : "PROD";
      try {
        fabricType = FabricType.valueOf(fabricTypeString);
      } catch (IllegalArgumentException e) {
        log.warn("Invalid env ({}). Setting env to default PROD", fabricTypeString);

      }
    }

    return fabricType;
  }

}
