package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * A class that holds all the registered {@link UrnPathExtractor}s.
 *
 * Register new type of urn path extractors by adding them to {@link #REGISTERED_URN_PATH_EXTRACTORS}.
 */
public class RegisteredUrnPathExtractors {

  private static final Map<Class<? extends Urn>, UrnPathExtractor> REGISTERED_URN_PATH_EXTRACTORS = new HashMap() {
    {
      put(DatasetUrn.class, new DatasetUrnPathExtractor());
    }
  };

  private RegisteredUrnPathExtractors() {
    // Util class
  }

  @Nonnull
  public static UrnPathExtractor getUrnPathExtractor(@Nonnull Class<? extends Urn> clazz) {
    return REGISTERED_URN_PATH_EXTRACTORS.get(clazz);
  }

  // For testing purpose
  public static void registerUrnPathExtractor(@Nonnull Class<? extends Urn> clazz, @Nonnull UrnPathExtractor extractor) {
    if (REGISTERED_URN_PATH_EXTRACTORS.containsKey(clazz)) {
      throw new RuntimeException("Path extractor for " + clazz.getCanonicalName() + " already registered!");
    }
    REGISTERED_URN_PATH_EXTRACTORS.put(clazz, extractor);
  }
}
