package com.linkedin.metadata.resources.entity;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.mxe.SystemMetadata;
import java.util.Set;
import javax.annotation.Nullable;


public class ResourceUtils {
  private ResourceUtils() {

  }

  public static Set<String> getAllAspectNames(final EntityService entityService, final String entityName) {
    return entityService.getEntityAspectNames(entityName);
  }

  public static void tryIndexRunId(final Urn urn, final @Nullable SystemMetadata systemMetadata,
      final EntitySearchService entitySearchService) {
    if (systemMetadata != null && systemMetadata.hasRunId()) {
      entitySearchService.appendRunId(urn.getEntityType(), urn, systemMetadata.getRunId());
    }
  }
}
