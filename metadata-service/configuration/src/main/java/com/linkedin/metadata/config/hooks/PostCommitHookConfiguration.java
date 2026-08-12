package com.linkedin.metadata.config.hooks;

import lombok.Data;

/** POJO representing the "datahub.postCommitHook" configuration block in application.yaml. */
@Data
public class PostCommitHookConfiguration {
  private PostCommitHookBufferProperties buffer;
}
