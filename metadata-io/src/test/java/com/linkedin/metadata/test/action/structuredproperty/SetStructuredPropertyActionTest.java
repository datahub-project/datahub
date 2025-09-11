package com.linkedin.metadata.test.action.structuredproperty;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.service.StructuredPropertyService;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.definition.ActionType;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SetStructuredPropertyActionTest {

  private static final List<Urn> DATASET_URNS =
      ImmutableList.of(
          UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test,PROD)"),
          UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test1,PROD)"));

  private static final List<Urn> ALL_URNS = new ArrayList<>();

  static {
    ALL_URNS.addAll(DATASET_URNS);
  }

  private static final Map<String, List<String>> VALID_STRING_PARAMS =
      ImmutableMap.of(
          "values", ImmutableList.of("urn:li:structuredProperty:test.property"),
          "stringValues", ImmutableList.of("test value", "another value"));

  private static final Map<String, List<String>> VALID_NUMBER_PARAMS =
      ImmutableMap.of(
          "values", ImmutableList.of("urn:li:structuredProperty:test.property"),
          "numberValues", ImmutableList.of("123.45", "67.89"));

  private static final Map<String, List<String>> INVALID_MISSING_VALUES_PARAMS =
      ImmutableMap.of("values", ImmutableList.of("urn:li:structuredProperty:test.property"));

  private static final Map<String, List<String>> INVALID_BOTH_VALUES_PARAMS =
      ImmutableMap.of(
          "values", ImmutableList.of("urn:li:structuredProperty:test.property"),
          "stringValues", ImmutableList.of("test value"),
          "numberValues", ImmutableList.of("123.45"));

  private static final Map<String, List<String>> INVALID_EMPTY_STRING_VALUES_PARAMS =
      ImmutableMap.of(
          "values", ImmutableList.of("urn:li:structuredProperty:test.property"),
          "stringValues", ImmutableList.of());

  @Test
  public void testGetActionType() {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    SetStructuredPropertyAction action = new SetStructuredPropertyAction(service);

    Assert.assertEquals(ActionType.SET_STRUCTURED_PROPERTY, action.getActionType());
  }

  @Test
  public void testValidateValidStringParams() throws InvalidActionParamsException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    SetStructuredPropertyAction action = new SetStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(VALID_STRING_PARAMS);

    action.validate(params); // Should not throw
  }

  @Test
  public void testValidateValidNumberParams() throws InvalidActionParamsException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    SetStructuredPropertyAction action = new SetStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(VALID_NUMBER_PARAMS);

    action.validate(params); // Should not throw
  }

  @Test(expectedExceptions = InvalidActionParamsException.class)
  public void testValidateMissingValueParams() throws InvalidActionParamsException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    SetStructuredPropertyAction action = new SetStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(INVALID_MISSING_VALUES_PARAMS);

    action.validate(params);
  }

  @Test(expectedExceptions = InvalidActionParamsException.class)
  public void testValidateBothStringAndNumberParams() throws InvalidActionParamsException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    SetStructuredPropertyAction action = new SetStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(INVALID_BOTH_VALUES_PARAMS);

    action.validate(params);
  }

  @Test(expectedExceptions = InvalidActionParamsException.class)
  public void testValidateEmptyStringValues() throws InvalidActionParamsException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    SetStructuredPropertyAction action = new SetStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(INVALID_EMPTY_STRING_VALUES_PARAMS);

    action.validate(params);
  }

  @Test
  public void testApplyWithStringValues() throws InvalidOperandException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    SetStructuredPropertyAction action = new SetStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(VALID_STRING_PARAMS);
    List<Urn> singleUrn = ImmutableList.of(DATASET_URNS.get(0));

    action.apply(mock(OperationContext.class), singleUrn, params);

    // Verify that batchSetStructuredProperty was called with correct parameters
    ArgumentCaptor<Urn> structuredPropertyUrnCaptor = ArgumentCaptor.forClass(Urn.class);
    ArgumentCaptor<List<ResourceReference>> resourcesCaptor = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<List<StructuredPropertyValueAssignment>> assignmentsCaptor =
        ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<String> appSourceCaptor = ArgumentCaptor.forClass(String.class);

    verify(service)
        .batchSetStructuredProperty(
            any(OperationContext.class),
            structuredPropertyUrnCaptor.capture(),
            resourcesCaptor.capture(),
            assignmentsCaptor.capture(),
            appSourceCaptor.capture());

    Assert.assertEquals(
        "urn:li:structuredProperty:test.property",
        structuredPropertyUrnCaptor.getValue().toString());
    Assert.assertEquals(1, resourcesCaptor.getValue().size());
    Assert.assertEquals(DATASET_URNS.get(0), resourcesCaptor.getValue().get(0).getUrn());
    Assert.assertEquals(1, assignmentsCaptor.getValue().size());
    Assert.assertEquals(METADATA_TESTS_SOURCE, appSourceCaptor.getValue());
  }

  @Test
  public void testApplyWithNumberValues() throws InvalidOperandException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    SetStructuredPropertyAction action = new SetStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(VALID_NUMBER_PARAMS);
    List<Urn> singleUrn = ImmutableList.of(DATASET_URNS.get(0));

    action.apply(mock(OperationContext.class), singleUrn, params);

    // Verify that batchSetStructuredProperty was called
    verify(service)
        .batchSetStructuredProperty(
            any(OperationContext.class),
            any(Urn.class),
            any(List.class),
            any(List.class),
            eq(METADATA_TESTS_SOURCE));
  }

  @Test
  public void testApplyWithMultipleUrns() throws InvalidOperandException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    SetStructuredPropertyAction action = new SetStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(VALID_STRING_PARAMS);

    action.apply(mock(OperationContext.class), ALL_URNS, params);

    // Verify that batchSetStructuredProperty was called with all URNs
    ArgumentCaptor<List<ResourceReference>> resourcesCaptor = ArgumentCaptor.forClass(List.class);
    verify(service)
        .batchSetStructuredProperty(
            any(OperationContext.class),
            any(Urn.class),
            resourcesCaptor.capture(),
            any(List.class),
            eq(METADATA_TESTS_SOURCE));

    Assert.assertEquals(ALL_URNS.size(), resourcesCaptor.getValue().size());
  }

  @Test
  public void testApplyWithEmptyUrns() throws InvalidOperandException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    SetStructuredPropertyAction action = new SetStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(VALID_STRING_PARAMS);

    action.apply(mock(OperationContext.class), ImmutableList.of(), params);

    // Should not call the service method for empty URNs
    verify(service, never()).batchSetStructuredProperty(any(), any(), any(), any(), any());
  }
}
