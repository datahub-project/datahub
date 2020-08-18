package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.Urn;
import java.util.Map;
import javax.annotation.Nonnull;


public interface UrnPathExtractor<URN extends Urn> {
  @Nonnull
  Map<String, Object> extractPaths(@Nonnull URN urn);
}
