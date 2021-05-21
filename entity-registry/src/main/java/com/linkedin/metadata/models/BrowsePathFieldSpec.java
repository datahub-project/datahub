package com.linkedin.metadata.models;

import com.linkedin.data.schema.PathSpec;
import javax.annotation.Nonnull;


public class BrowsePathFieldSpec {

  private final PathSpec _path;

  public BrowsePathFieldSpec(@Nonnull final PathSpec path) {
    _path = path;
  }

  public PathSpec getPath() {
    return _path;
  }

}
