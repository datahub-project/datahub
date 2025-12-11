/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
