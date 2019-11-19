package com.linkedin.metadata.utils.elasticsearch;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.apache.commons.lang.StringUtils;


public class ElasticsearchUtil {

  public enum AccessCountType {
    LOW, MEDIUM, HIGH
  }
  // TODO: Move these to config eventually
  static final Map<String, Character> PLATFORM_DELIMITER_MAP = ImmutableMap.<String, Character>builder()
      .put("ambry", '.')
      .put("couchbase", '.')
      .put("dalids", '.')
      .put("espresso", '.')
      .put("external", '.')
      .put("followfeed", '.')
      .put("hdfs", '/')
      .put("hive", '.')
      .put("kafka", '.')
      .put("kafka-lc", '.')
      .put("mongo",  '.')
      .put("mysql", '.')
      .put("oracle", '.')
      .put("pinot", '.')
      .put("presto", '.')
      .put("seas-cloud", '.')
      .put("seas-deployed", '/')
      .put("seas-hdfs", '/')
      .put("teradata", '.')
      .put("ump", '.')
      .put("vector", '.')
      .put("venice", '.')
      .put("voldemort", '.')
      .build();

  private ElasticsearchUtil() {
  }

  /**
   * Given a dataset name, extract the prefix
   *
   * @param datasetName name of the dataset
   * @return prefix name from the dataset name
   */
  @Nonnull
  public static String getPrefixFromDatasetName(@Nonnull String datasetName) {
    Pattern pattern = Pattern.compile("^(\\/.*?)[\\/].*");
    Matcher matcher = pattern.matcher(datasetName);
    if (matcher.matches()) {
      return matcher.group(1);
    }
    return StringUtils.substringBefore(datasetName, ".");
  }

  @Nonnull
  public static String constructPath(@Nonnull String dataOrigin, @Nonnull String platform, @Nonnull String dataset) {
    String path;
    if (PLATFORM_DELIMITER_MAP.containsKey(platform)) {
      Character delimiter = PLATFORM_DELIMITER_MAP.get(platform);
      if (delimiter.equals('/')) {
        path = "/" + dataOrigin + "/" + platform + dataset;
      } else {
        path = ("/" + dataOrigin + "/" + platform + "/" + dataset).replace(delimiter, '/');
      }
    } else {
      path = "/" + dataOrigin + "/" + platform + "/" + dataset;
    }

    return path.toLowerCase();
  }

  @Nonnull
  public static AccessCountType getAccessType(long accessCount) {
    // TODO: move these ranges to a config file
    if (accessCount == 0) {
      return AccessCountType.LOW;
    } else if (accessCount > 10000) {
      return AccessCountType.HIGH;
    } else {
      return AccessCountType.MEDIUM;
    }
  }

}