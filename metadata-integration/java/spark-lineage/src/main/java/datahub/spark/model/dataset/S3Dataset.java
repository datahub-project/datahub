package datahub.spark.model.dataset;

import java.util.List;
import java.util.stream.Stream;

import com.linkedin.common.FabricType;
import com.typesafe.config.Config;

import datahub.spark.SparkConfigUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class S3Dataset extends SparkDataset {



  public S3Dataset(String pathUri, String platformInstance, FabricType fabricType) {
    super("s3", platformInstance, pathUri, fabricType);
  }

  public S3Dataset(String pathUri) {
    super("s3", pathUri);
  }

  public static boolean isMatch(String pathUri) {
    return isS3Uri(pathUri);
  }

  public static S3Dataset create(String pathUri) throws InstantiationException {
    if (!isMatch(pathUri)) {
      throw new InstantiationException("S3Dataset instance cannot be created for non S3 pathUri: " + pathUri);
    }
    List<String> pathAliasList = SparkConfigUtil.getS3PathAliasList();
    log.debug("path_alias_list: " + String.join(",", pathAliasList));
    if (pathAliasList != null && pathAliasList.size() > 0) {
      for (String pathAlias : SparkConfigUtil.getS3PathAliasList()) {
        log.debug("Checking match for path_alias: " + pathAlias);
        Config pathSpecConfig = SparkConfigUtil.getS3PathAliasDetails(pathAlias);
        if (pathSpecConfig != null) {
          String s3Uri = getS3Uri(pathUri, SparkConfigUtil.getPathSpecList(pathSpecConfig));
          if (s3Uri != null) {
            String platformInstance = SparkConfigUtil.getPlatformInstance(pathSpecConfig);
            FabricType fabricType = SparkConfigUtil.getFabricType(pathSpecConfig);
            return new S3Dataset(getDatasetName(s3Uri), platformInstance, fabricType);
          }
        }

      }
      log.debug("nothing matched from path_alias_list. Falling back using config 'path_spec_list'");
    }
    String s3Uri = getS3Uri(pathUri, SparkConfigUtil.getS3PathSpecList());
    if (s3Uri == null) {
      s3Uri = pathUri;
    }
    return new S3Dataset(getDatasetName(s3Uri));
  }

  private static String getDatasetName(String s3Uri) {
    return stripS3Prefix(s3Uri);
  }

  private static String getS3Uri(String pathUri, List<String> pathSpecs) {

    if (pathSpecs == null || pathSpecs.size() == 0) {
      log.debug("No path_spec_list configuration found. Falling back to creating dataset name with complete s3 uri");
    } else {
      for (String pathSpec : pathSpecs) {
        String uri = PathUriNormalizationUtil.getMatchedUri(pathUri, pathSpec);
        if (uri != null) {
          return uri;
        }
      }
    }
    return null;
  }

 

  private static boolean isS3Uri(String uri) {
    return getS3PrefixStream().anyMatch(s -> uri.startsWith(s));
  }

  private static String stripS3Prefix(String uri) {
    return getS3PrefixStream().filter(p -> uri.startsWith(p)).map(q -> uri.replaceFirst("^" + q, ""))
        .findFirst().orElse(uri);
  }

  private static Stream<String> getS3PrefixStream() {
    return Stream.of("s3://", "s3n://", "s3a://");
  }
}
