package com.linkedin.metadata.resources.entity;

import com.linkedin.metadata.entity.EntityService;
import java.util.Set;

public class ResourceUtils {
  private ResourceUtils() {}

  public static Set<String> getAllAspectNames(
      final EntityService entityService, final String entityName) {
    return entityService.getEntityAspectNames(entityName);
  }
}
