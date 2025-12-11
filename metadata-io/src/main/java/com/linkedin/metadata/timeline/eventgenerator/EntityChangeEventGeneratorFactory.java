/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.timeline.eventgenerator;

import com.linkedin.metadata.timeline.data.ChangeCategory;
import java.util.HashMap;
import java.util.Map;

/**
 * A cheap factory for generating EntityChangeEvents, keyed by entity-type, element-type,
 * aspect-name
 */
public class EntityChangeEventGeneratorFactory {

  private final Map<String, EntityChangeEventGenerator> _entityChangeEventGeneratorMap =
      new HashMap<>();

  public void addGenerator(
      String entityName,
      ChangeCategory elementName,
      String aspectName,
      EntityChangeEventGenerator differ) {
    _entityChangeEventGeneratorMap.put(entityName + elementName.name() + aspectName, differ);
  }

  public EntityChangeEventGenerator getGenerator(
      String entityName, ChangeCategory category, String aspectName) {
    return _entityChangeEventGeneratorMap.get(entityName + category.name() + aspectName);
  }
}
