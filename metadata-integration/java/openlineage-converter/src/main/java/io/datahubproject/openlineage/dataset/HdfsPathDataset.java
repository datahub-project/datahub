package io.datahubproject.openlineage.dataset;

import com.linkedin.common.FabricType;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

@ToString
@Slf4j
public class HdfsPathDataset extends SparkDataset {
  private final String datasetPath;

  private static final String TABLE = "{table}";
  private static final String TABLE_MARKER = "/{table}";
  private static final String TABLE_MARKER_REGEX = "/\\{table\\}";

  private static final String URI_SPLITTER = "://";

  public HdfsPathDataset(
      String platform,
      String name,
      String platformInstance,
      FabricType fabricType,
      String datasetPath) {
    super(platform, platformInstance, name, fabricType);
    this.datasetPath = datasetPath;
  }

  public HdfsPathDataset(String pathUri, String platformInstance, FabricType fabricType) {
    super("hdfs", platformInstance, pathUri, fabricType);
    this.datasetPath = pathUri;
  }

  public HdfsPathDataset(String pathUri, DatahubOpenlineageConfig datahubConf) {
    super("hdfs", pathUri, datahubConf);
    this.datasetPath = pathUri;
  }

  public HdfsPathDataset(
      String platform, String name, String datasetPath, DatahubOpenlineageConfig datahubConf) {
    super(platform, name, datahubConf);
    this.datasetPath = datasetPath;
  }

  public String getDatasetPath() {
    return datasetPath;
  }

  public static HdfsPathDataset create(URI path, DatahubOpenlineageConfig datahubConf)
      throws InstantiationException {
    String pathUri = path.toString();
    pathUri = StringUtils.stripEnd(pathUri, "/");
    String platform;
    try {
      platform = getPlatform(pathUri);

      if (datahubConf.getPathSpecs() == null) {
        log.info("No path_spec_list configuration found for platform {}.", platform);
      } else {

        // Filter out path specs that don't match the platform
        for (PathSpec pathSpec : datahubConf.getPathSpecsForPlatform(platform)) {
          log.debug("Checking match for path_alias: " + pathSpec.getAlias());

          String rawName = getRawNameFromUri(pathUri, pathSpec.getPathSpecList());
          if (rawName != null) {
            String platformInstance = pathSpec.platformInstance.orElse(null);
            FabricType fabricType = datahubConf.getFabricType();
            return new HdfsPathDataset(
                platform, getDatasetName(rawName), platformInstance, fabricType, rawName);
          }
        }
      }
      if (datahubConf.getPathSpecs() == null) {
        log.info("No path_spec_list configuration found for platform {}.", platform);
      }
      String rawName = getRawNameFromUri(pathUri, null);
      if (rawName == null) {
        String partitionRegexp = datahubConf.getFilePartitionRegexpPattern();
        if (partitionRegexp != null) {
          rawName = getRawNameWithoutPartition(pathUri, partitionRegexp);
        } else {
          rawName = pathUri;
        }
      }
      String datasetName = getDatasetName(rawName);

      // If platform is file then we want to keep the trailing slash
      if (platform.equals("file")) {
        datasetName = stripPrefix(rawName);
      }
      return new HdfsPathDataset(platform, datasetName, rawName, datahubConf);
    } catch (IllegalArgumentException e) {
      return new HdfsPathDataset("hdfs", pathUri, pathUri, datahubConf);
    }
  }

  private static String getDatasetName(String rawName) throws IllegalArgumentException {
    return stripPrefix(rawName).replaceFirst("^/", "");
  }

  private static String getRawNameFromUri(String pathUri, List<String> pathSpecs) {

    if (pathSpecs == null || pathSpecs.isEmpty()) {
      log.info(
          "No path_spec_list configuration found. Falling back to creating dataset name with complete uri");
    } else {
      for (String pathSpec : pathSpecs) {
        String uri = getMatchedUri(pathUri, pathSpec);
        if (uri != null) {
          return uri;
        }
      }
      log.info(
          "None of the path specs matched for path {} from pathSpecs: {}.",
          pathUri,
          String.join(",", pathSpecs));
    }
    return null;
  }

  private static String getRawNameWithoutPartition(String pathUri, String partitionRegexp) {
    String result = pathUri.replaceAll(partitionRegexp + "$", "");
    // Remove trailing slash
    return result.replaceAll("/$", "");
  }

  private static String[] getSplitUri(String pathUri) throws IllegalArgumentException {
    if (pathUri.contains(URI_SPLITTER)) {
      String[] split = pathUri.split(URI_SPLITTER);
      if (split.length == 2) {
        return split;
      }
    }
    throw new IllegalArgumentException("Path URI is not as per expected format: " + pathUri);
  }

  private static String getPlatform(String pathUri) throws IllegalArgumentException {
    String prefix = getSplitUri(pathUri)[0];
    return HdfsPlatform.getPlatformFromPrefix(prefix);
  }

  private static String stripPrefix(String pathUri) throws IllegalArgumentException {

    return getSplitUri(pathUri)[1];
  }

  static String getMatchedUri(String pathUri, String pathSpec) {
    if (pathSpec.contains(TABLE_MARKER)) {
      String miniSpec = pathSpec.split(TABLE_MARKER_REGEX)[0] + TABLE_MARKER;
      String[] specFolderList = miniSpec.split("/");
      String[] pathFolderList = pathUri.split("/");
      StringBuilder uri = new StringBuilder();
      if (pathFolderList.length >= specFolderList.length) {
        for (int i = 0; i < specFolderList.length; i++) {
          if (specFolderList[i].equals(pathFolderList[i]) || specFolderList[i].equals("*")) {
            uri.append(pathFolderList[i]).append("/");
          } else if (specFolderList[i].equals(TABLE)) {
            uri.append(pathFolderList[i]);
            log.debug("Actual path [" + pathUri + "] matched with path_spec [" + pathSpec + "]");
            return uri.toString();
          } else {
            break;
          }
        }
      }
      log.debug("No path spec matched with actual path [" + pathUri + "]");
    } else {
      log.warn("Invalid path spec [" + pathSpec + "]. Path spec should contain {table}");
    }
    return null;
  }

  public enum HdfsPlatform {
    S3(Arrays.asList("s3", "s3a", "s3n"), "s3"),
    GCS(Arrays.asList("gs", "gcs"), "gcs"),
    ABFS(Arrays.asList("abfs", "abfss"), "abfs"),
    DBFS(Collections.singletonList("dbfs"), "dbfs"),
    FILE(Collections.singletonList("file"), "file"),
    // default platform
    HDFS(Collections.emptyList(), "hdfs");

    public final List<String> prefixes;
    public final String platform;

    HdfsPlatform(List<String> prefixes, String platform) {
      this.prefixes = prefixes;
      this.platform = platform;
    }

    public static String getPlatformFromPrefix(String prefix) {
      for (HdfsPlatform e : values()) {
        if (e.prefixes.contains(prefix)) {
          return e.platform;
        }
      }
      return HDFS.platform;
    }
  }
}
