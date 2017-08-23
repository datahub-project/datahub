/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package wherehows.utils;

import com.google.common.collect.ImmutableList;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import wherehows.common.Constant;


public class JobsUtil {

  // Patterns for environmental variables resolution in jobs file.
  private static final List<Pattern> ENV_VAR_PATTERNS = ImmutableList.<Pattern>builder()
      .add(Pattern.compile("\\$(.+)")) // $ENV_VAR
      .add(Pattern.compile("\\$\\{(.+)\\}")) // ${ENV_VAR}
      .build();

  /**
   * Reads {@link Properties} from the given file and resolves all environmental variables denoted by ${ENV_VAR_NAME}.
   *
   * @param jobFile Path to the property file.
   * @return Resolved {@link Properties} or null if failed to read from file.
   */
  public static Properties getResolvedProperties(Path jobFile) {
    Properties prop = new Properties();
    try (BufferedReader reader = Files.newBufferedReader(jobFile)) {
      prop.load(reader);
    } catch (IOException ex) {
      return null;
    }

    for (Map.Entry<Object, Object> entry : prop.entrySet()) {
      String value = (String) entry.getValue();
      prop.setProperty((String) entry.getKey(), resolveEnviornmentalVariable(value));
    }
    return prop;
  }

  /**
   * Resolves the value to the corresponding environmental variable if possible, otherwise returns the original value.
   */
  private static String resolveEnviornmentalVariable(String value) {
    for (Pattern pattern : ENV_VAR_PATTERNS) {
      Matcher matcher = pattern.matcher(value);
      if (matcher.matches()) {
        String resolved = System.getenv(matcher.group(1));
        if (resolved != null) {
          return resolved;
        }
      }
    }
    return value;
  }

  private static String jobNameFromFile(File file) {
    String filename = file.getName();
    return filename.substring(0, filename.lastIndexOf('.'));
  }

  /**
   * Returns a map of job name to job properties for all scheduled and enabled jobs.
   */
  public static Map<String, Properties> getScheduledJobs(String dir) {
    Map<String, Properties> jobs = new HashMap<>();
    for (File file : new File(dir).listFiles()) {
      if (file.getAbsolutePath().endsWith(".job")) {
        Properties prop = getResolvedProperties(file.toPath());
        if (!prop.containsKey(Constant.JOB_DISABLED_KEY) && prop.containsKey(Constant.JOB_CRON_EXPR_KEY)) {
          // job name = file name without the extension.
          jobs.put(jobNameFromFile(file), prop);
        }
      }
    }
    return jobs;
  }

  /**
   * Returns a map of job name to job properties for kafka jobs.
   */
  public static Map<String, Properties> getEnabledJobsByType(String dir, String type) {
    Map<String, Properties> jobs = new HashMap<>();
    for (File file : new File(dir).listFiles()) {
      if (file.getAbsolutePath().endsWith(".job")) {
        Properties prop = getResolvedProperties(file.toPath());
        if (!prop.containsKey(Constant.JOB_DISABLED_KEY) && prop.getProperty(Constant.JOB_TYPE_KEY, "").equals(type)) {
          // job name = file name without the extension.
          jobs.put(jobNameFromFile(file), prop);
        }
      }
    }
    return jobs;
  }
}
