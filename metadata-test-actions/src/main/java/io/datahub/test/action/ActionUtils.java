package io.datahub.test.action;

import com.linkedin.common.urn.Urn;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ActionUtils {

  public static Map<String, List<Urn>> getEntityTypeToUrns(List<Urn> urns) {
    final Map<String, List<Urn>> result = new HashMap<>();
    for (Urn entityUrn : urns) {
      result.putIfAbsent(entityUrn.getEntityType(), new ArrayList<>());
      result.get(entityUrn.getEntityType()).add(entityUrn);
    }
    return result;
  }

  private ActionUtils() {

  }
}
