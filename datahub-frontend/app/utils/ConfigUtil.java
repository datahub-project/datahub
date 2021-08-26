package utils;

import com.typesafe.config.Config;


public class ConfigUtil {
  public static boolean getBoolean(Config config, String key) {
    return config.hasPath(key) && config.getBoolean(key);
  }

  public static int getInt(Config config, String key, int defaultValue) {
    return config.hasPath(key) ? config.getInt(key) : defaultValue;
  }

  public static String getString(Config config, String key, String defaultValue) {
    return config.hasPath(key) ? config.getString(key) : defaultValue;
  }
}
