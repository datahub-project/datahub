package com.linkedin.metadata.dao.utils;

import com.linkedin.testing.urn.BazUrn;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;


public class BazUrnPathExtractor implements UrnPathExtractor<BazUrn> {
  @Override
  public Map<String, Object> extractPaths(@Nonnull BazUrn urn) {
    return Collections.unmodifiableMap(new HashMap() {
      {
        put("/bazId", urn.getBazIdEntity());
      }
    });
  }
}
