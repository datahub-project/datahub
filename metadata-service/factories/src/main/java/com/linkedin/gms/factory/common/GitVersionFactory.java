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
