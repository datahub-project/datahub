package datahub.spark.model.dataset;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PathUriNormalizationUtil {

  private PathUriNormalizationUtil() {

  }

  private static final String TABLE = "{table}";
  private static final String TABLE_MARKER = "/{table}";
  private static final String TABLE_MARKER_REGEX = "/\\{table\\}";

/* This utility method normalizes the pathUri as per pathSpec.
 * pathSpec must contain {table} modifier
 * If pathUri cannot be normalized then null value is returned.
 */
  
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

}
