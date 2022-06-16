package com.linkedin.metadata.test.eval;

import com.linkedin.metadata.test.definition.operation.OperationParam;
import java.util.Map;
import lombok.Value;


@Value
public class ResolvedParams {
  Map<String, ResolvedParam> params;

  // Utility function to check whether there is a param of with input key of input type
  public boolean hasKeyOfType(String key, OperationParam.Type type) {
    return params.containsKey(key) && params.get(key).getType() == type;
  }

  public ResolvedParam getResolvedParam(String key) {
    return params.get(key);
  }
}
