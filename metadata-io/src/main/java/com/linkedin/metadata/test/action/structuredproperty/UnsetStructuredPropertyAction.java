package com.linkedin.metadata.test.action.structuredproperty;

import static com.linkedin.metadata.Constants.METADATA_TESTS_SOURCE;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.service.StructuredPropertyService;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.action.api.ValuesAction;
import com.linkedin.metadata.test.definition.ActionType;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class UnsetStructuredPropertyAction extends ValuesAction {

  private final StructuredPropertyService structuredPropertyService;

  @Override
  public ActionType getActionType() {
    return ActionType.UNSET_STRUCTURED_PROPERTY;
  }

  @Override
  public void validate(ActionParameters params) throws InvalidActionParamsException {
    // Validate that values parameter contains the structured property URN
    super.validate(params);
  }

  @Override
  public void apply(@Nonnull OperationContext opContext, List<Urn> urns, ActionParameters params)
      throws InvalidOperandException {
    if (urns.isEmpty()) return;

    try {
      // Get the structured property URN from values parameter (first value)
      final List<String> values = params.getParams().get(VALUES_PARAM);
      final Urn structuredPropertyUrn = UrnUtils.getUrn(values.get(0));

      // Unset the structured property from all entities
      this.structuredPropertyService.batchUnsetStructuredProperty(
          opContext,
          structuredPropertyUrn,
          this.getResourceReferences(urns),
          METADATA_TESTS_SOURCE);

      log.info(
          "Successfully unset structured property {} for {} entities",
          structuredPropertyUrn,
          urns.size());
    } catch (Exception e) {
      log.error("Failed to unset structured property for entities: {}", e.getMessage(), e);
      throw new InvalidOperandException(
          "Failed to unset structured property: " + e.getMessage(), e);
    }
  }
}
