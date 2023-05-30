package com.linkedin.metadata.timeline.eventgenerator;

import com.linkedin.metadata.timeline.data.ChangeCategory;
import java.util.HashMap;
import java.util.Map;


/**
 * A cheap factory for generating EntityChangeEvents, keyed by entity-type, element-type, aspect-name
 */
public class EntityChangeEventGeneratorFactory {

  private final Map<String, EntityChangeEventGenerator> _entityChangeEventGeneratorMap = new HashMap<>();

  public void addGenerator(String entityName, ChangeCategory elementName, String aspectName,
      EntityChangeEventGenerator differ) {
    _entityChangeEventGeneratorMap.put(entityName + elementName.name() + aspectName, differ);
  }

  public EntityChangeEventGenerator getGenerator(String entityName, ChangeCategory category, String aspectName) {
    return _entityChangeEventGeneratorMap.get(entityName + category.name() + aspectName);
  }
}
