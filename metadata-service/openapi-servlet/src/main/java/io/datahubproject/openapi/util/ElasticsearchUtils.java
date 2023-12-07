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
