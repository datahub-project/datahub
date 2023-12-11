package com.linkedin.datahub.upgrade;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class UpgradeUtils {

  private static final String KEY_VALUE_DELIMITER = "=";

  public static Map<String, Optional<String>> parseArgs(final List<String> args) {
    if (args == null) {
      return Collections.emptyMap();
    }
    final Map<String, Optional<String>> parsedArgs = new HashMap<>();

    for (final String arg : args) {
      List<String> parsedArg = Arrays.asList(arg.split(KEY_VALUE_DELIMITER, 2));
      parsedArgs.put(
          parsedArg.get(0),
          parsedArg.size() > 1 ? Optional.of(parsedArg.get(1)) : Optional.empty());
    }
    return parsedArgs;
  }

  private UpgradeUtils() {}
}
