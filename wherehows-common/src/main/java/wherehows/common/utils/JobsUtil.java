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
package wherehows.common.utils;

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import wherehows.common.Constant;


public class JobsUtil {

  // Patterns for environmental variables resolution in jobs file.
  private static final List<Pattern> ENV_VAR_PATTERNS =
      ImmutableList.<Pattern>builder().add(Pattern.compile("\\$(.+)")) // $ENV_VAR
          .add(Pattern.compile("\\$\\{(.+)\\}")) // ${ENV_VAR}
          .build();

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

  /**
   * Get job name without file extension
   * @param file File
   * @return String
   */
  public static String jobNameFromFile(File file) {
    return jobNameFromFileName(file.getName());
  }

  /**
   * Get job name without file extension
   * @param path Path
   * @return String
   */
  public static String jobNameFromPath(Path path) {
    return jobNameFromFileName(path.getFileName().toString());
  }

  /**
   * Get job name without file extension
   * @param filename String
   * @return String
   */
  public static String jobNameFromFileName(String filename) {
    return filename.substring(0, filename.lastIndexOf('.'));
  }

  /**
   * Returns a map of job name to job properties for all scheduled and enabled jobs.
   */
  public static Map<String, Properties> getScheduledJobs(String dir) throws ConfigurationException {
    Map<String, Properties> jobs = new HashMap<>();
    for (File file : new File(dir).listFiles()) {
      if (file.getAbsolutePath().endsWith(".job")) {
        Properties prop = getJobConfigProperties(file);
        if (!prop.containsKey(Constant.JOB_DISABLED_KEY) && prop.containsKey(Constant.JOB_CRON_EXPR_KEY)) {
          // job name = file name without the extension.
          jobs.put(jobNameFromFile(file), prop);
        }
      }
    }
    return jobs;
  }

  /**
   * Returns a map of job name to job properties which are enabled.
   */
  public static Map<String, Properties> getEnabledJobs(String dir) throws ConfigurationException {
    Map<String, Properties> jobs = new HashMap<>();
    for (File file : new File(dir).listFiles()) {
      if (file.getAbsolutePath().endsWith(".job")) {
        Properties prop = getJobConfigProperties(file);
        if (!prop.containsKey(Constant.JOB_DISABLED_KEY)) {
          // job name = file name without the extension.
          jobs.put(jobNameFromFile(file), prop);
        }
      }
    }
    return jobs;
  }

  /**
   * Returns a map of job name to job properties which are enabled and of certain type.
   */
  public static Map<String, Properties> getEnabledJobsByType(String dir, String type) throws ConfigurationException {
    Map<String, Properties> jobs = new HashMap<>();
    for (File file : new File(dir).listFiles()) {
      if (file.getAbsolutePath().endsWith(".job")) {
        Properties prop = getJobConfigProperties(file);
        if (!prop.containsKey(Constant.JOB_DISABLED_KEY) && prop.getProperty(Constant.JOB_TYPE_KEY, "").equals(type)) {
          // job name = file name without the extension.
          jobs.put(jobNameFromFile(file), prop);
        }
      }
    }

    return jobs;
  }

  public static Properties getJobConfigProperties(File jobConfigFile) throws ConfigurationException {
    Properties prop = new Properties();
    String propValues = "";

    Configuration jobParam = new PropertiesConfiguration(jobConfigFile.getAbsolutePath());

    if (jobParam != null) {
      Iterator<String> keyit = jobParam.getKeys();
      while (keyit.hasNext()) {
        String key = keyit.next();
        if (key != null) {
          Object value = jobParam.getProperty(key);
          if (value != null && value instanceof String[]) {
            propValues = String.join(",", (String[]) value);
            prop.setProperty(key, splitAndResolveEnvironmentalValues(propValues));
          } else if (value != null && value instanceof List<?>) {
            propValues = String.join(",", (List<String>) value);
            prop.setProperty(key, splitAndResolveEnvironmentalValues(propValues));
          } else if (value != null) {
            prop.setProperty(key, resolveEnviornmentalVariable(value.toString()));
          }
        }
      }
    }
    return prop;
  }

  public static String splitAndResolveEnvironmentalValues(String propValues) {

    String[] values = propValues.split(",");
    String commaValues = "";
    String indexValue = "";
    for (int i = 0; i < values.length; i++) {
      indexValue = resolveEnviornmentalVariable(values[i]);
      values[i] = indexValue;
    }
    return String.join(",", (String[]) values);
  }
}
