package com.linkedin.metadata.utils.mxe;

import com.linkedin.common.urn.Urn;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import javax.annotation.Nonnull;


public class EntityKeyUtils {
  private EntityKeyUtils() {
  }

  @Nonnull
  public static Urn getUrnFromProposal(MetadataChangeProposal metadataChangeProposal) {
    if (metadataChangeProposal.hasEntityUrn() && metadataChangeProposal.hasEntityKeyAspect()) {
      throw new IllegalArgumentException("Urn and keyAspect cannot both be set");
    }
    if (metadataChangeProposal.hasEntityUrn()) {
      return metadataChangeProposal.getEntityUrn();
    }
    if (metadataChangeProposal.hasEntityKeyAspect()) {
      throw new UnsupportedOperationException("Identifying entity with key aspect is not yet supported");
    }
    throw new IllegalArgumentException("One of urn and keyAspect must be set");
  }

  @Nonnull
  public static Urn getUrnFromLog(MetadataChangeLog metadataChangeLog) {
    if (metadataChangeLog.hasEntityUrn() && metadataChangeLog.hasEntityKeyAspect()) {
      throw new IllegalArgumentException("Urn and keyAspect cannot both be set");
    }
    if (metadataChangeLog.hasEntityUrn()) {
      return metadataChangeLog.getEntityUrn();
    }
    if (metadataChangeLog.hasEntityKeyAspect()) {
      throw new UnsupportedOperationException("Identifying entity with key aspect is not yet supported");
    }
    throw new IllegalArgumentException("One of urn and keyAspect must be set");
  }
}
