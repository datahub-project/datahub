package com.linkedin.metadata.service.docimport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;

/** Metadata about a file in a GitHub repository. */
@Value
public class GitHubFileInfo {
  @Nonnull String path;
  @Nullable Integer size;
}
