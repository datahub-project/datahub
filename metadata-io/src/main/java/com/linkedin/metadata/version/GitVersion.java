package com.linkedin.metadata.version;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Value;

@Value
public class GitVersion {
  String version;
  String commitId;
  Optional<String> flag;

  public static GitVersion getVersion(@Nonnull String commitId, @Nonnull String commitDescribe) {
    String version = commitDescribe.split("-")[0];
    return new GitVersion(version, commitId, Optional.empty());
  }

  public Map<String, Object> toConfig() {
    return ImmutableMap.of("version", version, "commit", commitId);
  }
}
