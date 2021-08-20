package react.auth;

public class ConfigUtil {

  private ConfigUtil() { }

  public static String getRequired(
      final com.typesafe.config.Config configs,
      final String path) {
    if (!configs.hasPath(path)) {
      throw new IllegalArgumentException(
          String.format("Missing required config with path %s", path));
    }
    return configs.getString(path);
  }

  public static String getOptional
      (final com.typesafe.config.Config configs,
      final String path,
      final String defaultVal) {
    if (!configs.hasPath(path)) {
      return defaultVal;
    }
    return configs.getString(path);
  }

}
