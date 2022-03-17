package com.linkedin.metadata.timeline.differ;

import com.linkedin.metadata.timeline.data.ChangeCategory;
import java.util.HashMap;
import java.util.Map;


/**
 * A cheap factory for diff engines, keyed by entity-type, element-type, aspect-name
 */
public class DiffFactory {

  private final Map<String, Differ> _differMap = new HashMap<>();

  public void addDiffer(String dataset, ChangeCategory elementName, String aspectName, Differ differ) {
    _differMap.put(dataset + elementName.name() + aspectName, differ);
  }

  public Differ getDiffer(String dataset, ChangeCategory elementName, String aspectName) {
    return _differMap.get(dataset + elementName.name() + aspectName);
  }

}
