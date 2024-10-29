package io.datahubproject;

import io.swagger.codegen.v3.generators.java.SpringCodegen;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CustomSpringCodegen extends SpringCodegen {

  public CustomSpringCodegen() {
    super();
  }

  @Override
  public String getName() {
    return "custom-spring";
  }

  @Override
  public Map<String, Object> postProcessOperations(Map<String, Object> objs) {
    Map<String, Object> result = super.postProcessOperations(objs);
    List<Map<String, String>> imports = (List) objs.get("imports");

    for (Map<String, String> importMap : imports) {
      for (String type : importMap.values()) {
        if (type.contains("EntityRequest") && !type.contains(".Scroll")) {
          additionalProperties.put("requestClass", type);
        }
        if (type.contains("EntityResponse") && !type.contains(".Scroll")) {
          additionalProperties.put("responseClass", type);
        }
        if (type.contains("EntityResponse") && type.contains(".Scroll")) {
          additionalProperties.put("scrollResponseClass", type);
        }
      }
    }

    return result;
  }
}
