package auth;

import com.typesafe.config.Config;
import java.util.Optional;

public class ConfigUtil {

  private ConfigUtil() {}

  public static String getRequired(final Config configs, final String path) {
    if (!configs.hasPath(path)) {
      throw new IllegalArgumentException(
          String.format("Missing required config with path %s", path));
    }
    return configs.getString(path);
  }

  public static String getOptional(
      final Config configs, final String path, final String defaultVal) {
    if (!configs.hasPath(path)) {
      return defaultVal;
    }
    return configs.getString(path);
  }

  public static Optional<String> getOptional(final Config configs, final String path) {
    if (!configs.hasPath(path)) {
      return Optional.empty();
    }
    return Optional.of(configs.getString(path));
  }
}
