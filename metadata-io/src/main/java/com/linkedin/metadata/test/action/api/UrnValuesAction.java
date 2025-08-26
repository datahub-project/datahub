package com.linkedin.metadata.test.action.api;

import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import java.util.List;
import java.util.Set;

public abstract class UrnValuesAction extends ValuesAction {

  protected Set<String> validValueEntityTypes() {
    return null;
  }

  @Override
  public void validate(ActionParameters params) throws InvalidActionParamsException {
    super.validate(params);
    if (validValueEntityTypes() != null) {
      List<String> urnStrings = params.getParams().get(VALUES_PARAM);

      if (!urnStrings.stream()
          .allMatch(
              urnStr ->
                  urnStr != null
                      && validValueEntityTypes().stream()
                          .anyMatch(
                              entityType ->
                                  urnStr.startsWith(String.format("urn:li:%s:", entityType))))) {
        throw new InvalidActionParamsException(
            String.format(
                "Action parameters are missing the required entity type urns in the 'values' parameter. Expected types: %s, Found URNs: %s",
                validValueEntityTypes(), urnStrings));
      }
    }
  }
}
