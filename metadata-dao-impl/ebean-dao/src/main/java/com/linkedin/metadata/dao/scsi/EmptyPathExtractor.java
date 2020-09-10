package com.linkedin.metadata.dao.scsi;

import com.linkedin.common.urn.Urn;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * A path extractor which does nothing.
 */
public final class EmptyPathExtractor<URN extends Urn> implements UrnPathExtractor<URN> {
  @Nonnull
  @Override
  public Map<String, Object> extractPaths(@Nonnull URN urn) {
    return Collections.emptyMap();
  }
}
