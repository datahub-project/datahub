package datahub.spark.model.dataset;

import java.util.Arrays;
import java.util.List;

import javax.xml.bind.ValidationException;

import org.apache.hadoop.fs.Path;

import com.linkedin.common.FabricType;
import com.typesafe.config.Config;

import datahub.spark.SparkConfigUtil;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@ToString
@Slf4j
public class HdfsPathDataset extends SparkDataset {

  private static final String TABLE = "{table}";
  private static final String TABLE_MARKER = "/{table}";
  private static final String TABLE_MARKER_REGEX = "/\\{table\\}";

  private static final String URI_SPLITTER = "://";

  public HdfsPathDataset(String platform, String name, String platformInstance,
      FabricType fabricType) {
    super(platform, platformInstance, name, fabricType);
  }

  public HdfsPathDataset(Path path, String platformInstance, FabricType fabricType) {
    this(path.toUri().toString(), platformInstance, fabricType);
  }

  public HdfsPathDataset(String pathUri, String platformInstance, FabricType fabricType) {
    super("hdfs", platformInstance, pathUri, fabricType);
  }

  public HdfsPathDataset(String pathUri) {
    super("hdfs", pathUri);
  }

  public HdfsPathDataset(String platform, String name) {
    super(platform, name);
  }

  public static HdfsPathDataset create(Path path) throws InstantiationException {
    String pathUri = path.toUri().toString();
    String platform;
    try {
      platform = getPlatform(pathUri);

      List<String> pathAliasList = SparkConfigUtil.getPathAliasListForPlatform(platform);
      log.debug("path_alias_list: " + String.join(",", pathAliasList));
      if (pathAliasList != null && pathAliasList.size() > 0) {
        for (String pathAlias : pathAliasList) {
          log.debug("Checking match for path_alias: " + pathAlias);
          Config pathSpecConfig = SparkConfigUtil.getPathAliasDetails(pathAlias, platform);
          if (pathSpecConfig != null) {
            String rawName = getRawNameFromUri(pathUri, SparkConfigUtil.getPathSpecList(pathSpecConfig));
            if (rawName != null) {
              String platformInstance = SparkConfigUtil.getPlatformInstance(pathSpecConfig);
              FabricType fabricType = SparkConfigUtil.getFabricType(pathSpecConfig);
              return new HdfsPathDataset(platform, getDatasetName(rawName), platformInstance, fabricType);
            }
          }

        }
        log.debug("nothing matched from path_alias_list. Falling back using config 'path_spec_list'");
      }
      String rawName = getRawNameFromUri(pathUri, SparkConfigUtil.getPathSpecListForPlatform(platform));
      if (rawName == null) {
        rawName = pathUri;
      }
      return new HdfsPathDataset(platform, getDatasetName(rawName));
    } catch (ValidationException e) {
      return new HdfsPathDataset("hdfs", pathUri);
    }
  }

  private static String getDatasetName(String rawName) throws ValidationException {
    return stripPrefix(rawName).replaceFirst("^/", "");
  }

  private static String getRawNameFromUri(String pathUri, List<String> pathSpecs) {

    if (pathSpecs == null || pathSpecs.size() == 0) {
      log.debug("No path_spec_list configuration found. Falling back to creating dataset name with complete s3 uri");
    } else {
      for (String pathSpec : pathSpecs) {
        String uri = getMatchedUri(pathUri, pathSpec);
        if (uri != null) {
          return uri;
        }
      }
    }
    return null;
  }

  private static String[] getSplitUri(String pathUri) throws ValidationException {
    if (pathUri.contains(URI_SPLITTER)) {
      String[] split = pathUri.split(URI_SPLITTER);
      System.out.println("within " + String.join(",", split));
      if (split.length == 2) {
        return split;
      }
    }
    throw new ValidationException("Path URI is not as per expected format: {}", pathUri);

  }

//  private static String getPlatform(String pathUri) throws ValidationException {
//    String prefix = getSplitUri(pathUri)[0];
//    switch (prefix) {
//    case "s3":
//    case "s3a":
//    case "s3n":
//      return "s3";
//    case "gs":
//      return "gcs";
//    case "file":
//      return "local";
//    default:
//      return "hdfs";
//    }
//  }

  private static String getPlatform(String pathUri) throws ValidationException {
    String prefix = getSplitUri(pathUri)[0];
    return HdfsPlatform.getPlatformFromPrefix(prefix);
  }

  private static enum HdfsPlatform {
    S3(Arrays.asList("s3", "s3a", "s3n"), "s3"),
    GCS(Arrays.asList("gs"), "gcs"),
    // default platform
    HDFS(Arrays.asList(), "hdfs");

    public final List<String> prefixes;
    public final String platform;

    private HdfsPlatform(List<String> prefixes, String platform) {
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

  private static String stripPrefix(String pathUri) throws ValidationException {

    return getSplitUri(pathUri)[1];
  }

  static String getMatchedUri(String pathUri, String pathSpec) {
    if (pathSpec.contains(TABLE_MARKER)) {
      String miniSpec = pathSpec.split(TABLE_MARKER_REGEX)[0] + TABLE_MARKER;
      String[] specFolderList = miniSpec.split("/");
      String[] pathFolderList = pathUri.split("/");
      StringBuffer uri = new StringBuffer();
      if (pathFolderList.length >= specFolderList.length) {
        for (int i = 0; i < specFolderList.length; i++) {
          if (specFolderList[i].equals(pathFolderList[i]) || specFolderList[i].equals("*")) {
            uri.append(pathFolderList[i] + "/");
          } else if (specFolderList[i].equals(TABLE)) {
            uri.append(pathFolderList[i]);
            log.debug("Actual path [" + pathUri + "] matched with path_spec [" + pathSpec
                + "]");
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

  public static void main(String[] args) throws ValidationException {
    System.out.println(String.join(", ", getSplitUri("s3://mubuckt")));
    System.out.println(Arrays.asList("s3", "s3a", "s3n").contains("s3"));
  }

}
