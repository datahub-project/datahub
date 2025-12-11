/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.common;

import com.linkedin.metadata.version.GitVersion;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("classpath:git.properties")
public class GitVersionFactory {
  @Value("${git.commit.id}")
  private String commitId;

  @Value("${git.commit.id.describe}")
  private String commitDescribe;

  @Nonnull
  @Bean(name = "gitVersion")
  protected GitVersion getInstance() {
    return GitVersion.getVersion(commitId, commitDescribe);
  }
}
