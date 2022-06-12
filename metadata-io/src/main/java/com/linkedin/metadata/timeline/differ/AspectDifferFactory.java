package com.linkedin.metadata.timeline.differ;

import com.linkedin.metadata.timeline.data.ChangeCategory;
import java.util.HashMap;
import java.util.Map;


/**
 * A cheap factory for diff engines, keyed by entity-type, element-type, aspect-name
 */
public class AspectDifferFactory {

  private final Map<String, AspectDiffer> _differMap = new HashMap<>();

  public void addDiffer(String entityName, ChangeCategory elementName, String aspectName, AspectDiffer differ) {
    _differMap.put(entityName + elementName.name() + aspectName, differ);
  }

  public AspectDiffer getDiffer(String entityName, ChangeCategory category, String aspectName) {
    return _differMap.get(entityName + category.name() + aspectName);
  }
}
