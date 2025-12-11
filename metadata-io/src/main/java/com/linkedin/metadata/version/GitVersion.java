/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
