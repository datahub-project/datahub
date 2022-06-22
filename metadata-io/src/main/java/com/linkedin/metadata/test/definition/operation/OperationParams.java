package com.linkedin.metadata.test.definition.operation;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Value;


@Value
public class OperationParams {
  Map<String, OperationParam> params;

  // Utility function to check whether there is a param of with input key of input type
  public boolean hasKeyOfType(String key, OperationParam.Type type) {
    return params.containsKey(key) && params.get(key).getType() == type;
  }

  // Utility function to get param with input key of input paramClass. Returns empty if there is none
  public <T> Optional<T> getParamOfType(String key, Class<T> paramClass) {
    if (!params.containsKey(key) || !paramClass.isAssignableFrom(params.get(key).getClass())) {
      return Optional.empty();
    }
    return Optional.of(paramClass.cast(params.get(key)));
  }

  // Utility function to get all parameters with the input paramClass type
  public <T> List<T> getAllParamsOfType(Class<T> paramClass) {
    return params.values()
        .stream()
        .filter(param -> paramClass.isAssignableFrom(param.getClass()))
        .map(paramClass::cast)
        .collect(Collectors.toList());
  }
}
