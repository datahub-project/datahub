package io.datahubproject.openlineage.dataset;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public enum HdfsPlatform {
  S3(Arrays.asList("s3", "s3a", "s3n"), "s3"),
  GCS(Arrays.asList("gs", "gcs"), "gcs"),
  ABFS(Arrays.asList("abfs", "abfss"), "abfs"),
  DBFS(Collections.singletonList("dbfs"), "dbfs"),
  FILE(Collections.singletonList("file"), "file"),
  // default platform
  HDFS(Collections.emptyList(), "hdfs");

  public final List<String> prefixes;
  public final String platform;

  HdfsPlatform(List<String> prefixes, String platform) {
    this.prefixes = prefixes;
    this.platform = platform;
  }

  public static boolean isFsPlatformPrefix(String prefix) {
    for (HdfsPlatform e : values()) {
      if (e.prefixes.contains(prefix)) {
        return true;
      }
    }
    return false;
  }
}
