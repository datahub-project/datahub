/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.openapi.util;

public class ElasticsearchUtils {
  private ElasticsearchUtils() {}

  public static boolean isTaskIdValid(String task) {
    if (task.matches("^[a-zA-Z0-9-_]+:[0-9]+$")) {
      try {
        return Long.parseLong(task.split(":")[1]) != 0;
      } catch (NumberFormatException e) {
        return false;
      }
    }
    return false;
  }
}
