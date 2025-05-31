package io.openlineage.spark.agent.vendor.delta;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides utility methods to check whether Delta Lake classes are available in the
 * classpath and can be safely used.
 */
public class DeltaVendor {
  private static final Logger logger = LoggerFactory.getLogger(DeltaVendor.class);
  private static final List<String> DELTA_CLASSES =
      Collections.unmodifiableList(
          Arrays.asList(
              "io.delta.tables.DeltaTable",
              "org.apache.spark.sql.delta.commands.MergeIntoCommand"));
  private static boolean deltaChecked = false;
  private static boolean deltaAvailable = false;

  private DeltaVendor() {}

  /**
   * Determines whether Delta classes are available in the classpath.
   *
   * @return true if Delta classes are available
   */
  public static boolean hasDeltaClasses() {
    if (deltaChecked) {
      return deltaAvailable;
    }
    deltaChecked = true;
    Set<String> missingClasses = new HashSet<>();
    for (String className : DELTA_CLASSES) {
      try {
        Class.forName(className);
      } catch (ClassNotFoundException e) {
        missingClasses.add(className);
      }
    }
    if (missingClasses.isEmpty()) {
      deltaAvailable = true;
    } else {
      logger.info("The following Delta Lake classes are missing: {}", missingClasses);
      deltaAvailable = false;
    }
    return deltaAvailable;
  }
}
