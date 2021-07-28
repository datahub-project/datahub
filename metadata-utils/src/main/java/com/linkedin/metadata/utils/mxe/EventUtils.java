package com.linkedin.metadata.utils.mxe;

import com.linkedin.common.urn.Urn;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;


public class EventUtils {
  private EventUtils() {
  }

  public static Urn getUrnFromProposal(MetadataChangeProposal metadataChangeProposal) {
    return getUrnFromEntityKey(metadataChangeProposal.getEntityKey());
  }

  public static Urn getUrnFromLog(MetadataChangeLog metadataChangeLog) {
    return getUrnFromEntityKey(metadataChangeLog.getEntityKey());
  }

  private static Urn getUrnFromEntityKey(com.linkedin.mxe.MetadataChangeProposal.EntityKey entityKey) {
    if (entityKey.isGenericAspect()) {
      throw new UnsupportedOperationException("Key as generic struct is not yet supported");
    }
    return entityKey.getUrn();
  }
}
