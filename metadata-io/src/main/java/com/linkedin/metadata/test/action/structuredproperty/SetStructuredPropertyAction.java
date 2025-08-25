package com.linkedin.metadata.test.action.structuredproperty;

import static com.linkedin.metadata.Constants.METADATA_TESTS_SOURCE;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.service.StructuredPropertyService;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.definition.ActionType;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SetStructuredPropertyAction extends StructuredPropertyAbstractAction {

  private static final String STRING_VALUES_PARAMETER = "stringValues";
  private static final String NUMBER_VALUES_PARAMETER = "numberValues";

  public SetStructuredPropertyAction(StructuredPropertyService structuredPropertyService) {
    super(structuredPropertyService);
  }

  @Override
  public ActionType getActionType() {
    return ActionType.SET_STRUCTURED_PROPERTY;
  }

  @Override
  public void validate(ActionParameters params) throws InvalidActionParamsException {
    super.validate(params);
    
    // Check that either stringValues or numberValues is provided
    boolean hasStringValues = params.getParams().containsKey(STRING_VALUES_PARAMETER) 
        && params.getParams().get(STRING_VALUES_PARAMETER) != null 
        && !params.getParams().get(STRING_VALUES_PARAMETER).isEmpty();
    boolean hasNumberValues = params.getParams().containsKey(NUMBER_VALUES_PARAMETER) 
        && params.getParams().get(NUMBER_VALUES_PARAMETER) != null 
        && !params.getParams().get(NUMBER_VALUES_PARAMETER).isEmpty();

    if (!hasStringValues && !hasNumberValues) {
      throw new InvalidActionParamsException(
          "Action parameters are missing the required 'stringValues' or 'numberValues' parameter.");
    }

    if (hasStringValues && hasNumberValues) {
      throw new InvalidActionParamsException(
          "Action parameters cannot have both 'stringValues' and 'numberValues' parameters.");
    }
  }

  @Override
  void applyInternal(@Nonnull OperationContext opContext, Urn structuredPropertyUrn, List<Urn> urns, ActionParameters params) {
    try {
      // Create property value assignment from parameters
      StructuredPropertyValueAssignment assignment = createPropertyValueAssignment(structuredPropertyUrn, params);
      List<StructuredPropertyValueAssignment> assignments = List.of(assignment);

      // Apply to all entities
      this.structuredPropertyService.batchSetStructuredProperty(
          opContext, structuredPropertyUrn, getResourceReferences(urns), assignments, METADATA_TESTS_SOURCE);
      
      log.info("Successfully set structured property {} for {} entities", structuredPropertyUrn, urns.size());
    } catch (Exception e) {
      log.error("Failed to set structured property for entities: {}", e.getMessage(), e);
      throw new InvalidOperandException("Failed to set structured property: " + e.getMessage(), e);
    }
  }

  private StructuredPropertyValueAssignment createPropertyValueAssignment(Urn structuredPropertyUrn, ActionParameters params) {
    StructuredPropertyValueAssignment assignment = new StructuredPropertyValueAssignment();
    assignment.setPropertyUrn(structuredPropertyUrn);
    
    // Create values from parameters
    PrimitivePropertyValueArray values = new PrimitivePropertyValueArray();
    
    List<String> stringValues = params.getParams().get(STRING_VALUES_PARAMETER);
    List<String> numberValues = params.getParams().get(NUMBER_VALUES_PARAMETER);
    
    if (stringValues != null && !stringValues.isEmpty()) {
      values.addAll(
          stringValues.stream().map(PrimitivePropertyValue::create).collect(Collectors.toList()));
    } else if (numberValues != null && !numberValues.isEmpty()) {
      values.addAll(
          numberValues.stream()
              .map(Double::parseDouble)
              .map(PrimitivePropertyValue::create)
              .collect(Collectors.toList()));
    }
    
    assignment.setValues(values);
    return assignment;
  }
}
