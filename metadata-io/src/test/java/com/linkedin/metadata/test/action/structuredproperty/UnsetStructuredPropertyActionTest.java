package com.linkedin.metadata.test.action.structuredproperty;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
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
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UnsetStructuredPropertyActionTest {

  private static final List<Urn> DATASET_URNS =
      ImmutableList.of(
          UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test,PROD)"),
          UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test1,PROD)"));

  private static final List<Urn> ALL_URNS = new ArrayList<>();

  static {
    ALL_URNS.addAll(DATASET_URNS);
  }

  private static final Map<String, List<String>> VALID_PARAMS =
      ImmutableMap.of("values", ImmutableList.of("urn:li:structuredProperty:test.property"));

  @Test
  public void testGetActionType() {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    UnsetStructuredPropertyAction action = new UnsetStructuredPropertyAction(service);

    Assert.assertEquals(ActionType.UNSET_STRUCTURED_PROPERTY, action.getActionType());
  }

  @Test
  public void testValidateValidParams() throws InvalidActionParamsException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    UnsetStructuredPropertyAction action = new UnsetStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(VALID_PARAMS);

    action.validate(params); // Should not throw
  }

  @Test(expectedExceptions = InvalidActionParamsException.class)
  public void testValidateMissingValues() throws InvalidActionParamsException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    UnsetStructuredPropertyAction action = new UnsetStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(ImmutableMap.of());

    action.validate(params);
  }

  @Test(expectedExceptions = InvalidActionParamsException.class)
  public void testValidateEmptyValues() throws InvalidActionParamsException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    UnsetStructuredPropertyAction action = new UnsetStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(ImmutableMap.of("values", ImmutableList.of()));

    action.validate(params);
  }

  @Test
  public void testApply() throws InvalidOperandException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    UnsetStructuredPropertyAction action = new UnsetStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(VALID_PARAMS);
    List<Urn> singleUrn = ImmutableList.of(DATASET_URNS.get(0));

    action.apply(mock(OperationContext.class), singleUrn, params);

    // Verify that batchUnsetStructuredProperty was called with correct parameters
    ArgumentCaptor<Urn> structuredPropertyUrnCaptor = ArgumentCaptor.forClass(Urn.class);
    ArgumentCaptor<List<ResourceReference>> resourcesCaptor = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<String> appSourceCaptor = ArgumentCaptor.forClass(String.class);

    verify(service)
        .batchUnsetStructuredProperty(
            any(OperationContext.class),
            structuredPropertyUrnCaptor.capture(),
            resourcesCaptor.capture(),
            appSourceCaptor.capture());

    Assert.assertEquals(
        "urn:li:structuredProperty:test.property",
        structuredPropertyUrnCaptor.getValue().toString());
    Assert.assertEquals(1, resourcesCaptor.getValue().size());
    Assert.assertEquals(DATASET_URNS.get(0), resourcesCaptor.getValue().get(0).getUrn());
    Assert.assertEquals(METADATA_TESTS_SOURCE, appSourceCaptor.getValue());
  }

  @Test
  public void testApplyWithMultipleUrns() throws InvalidOperandException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    UnsetStructuredPropertyAction action = new UnsetStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(VALID_PARAMS);

    action.apply(mock(OperationContext.class), ALL_URNS, params);

    // Verify that batchUnsetStructuredProperty was called with all URNs
    ArgumentCaptor<List<ResourceReference>> resourcesCaptor = ArgumentCaptor.forClass(List.class);
    verify(service)
        .batchUnsetStructuredProperty(
            any(OperationContext.class),
            any(Urn.class),
            resourcesCaptor.capture(),
            eq(METADATA_TESTS_SOURCE));

    Assert.assertEquals(ALL_URNS.size(), resourcesCaptor.getValue().size());
  }

  @Test
  public void testApplyWithEmptyUrns() throws InvalidOperandException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    UnsetStructuredPropertyAction action = new UnsetStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(VALID_PARAMS);

    action.apply(mock(OperationContext.class), ImmutableList.of(), params);

    // Should not call the service method for empty URNs
    verify(service, never()).batchUnsetStructuredProperty(any(), any(), any(), any());
  }

  @Test(expectedExceptions = InvalidOperandException.class)
  public void testApplyHandlesServiceException() throws InvalidOperandException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    UnsetStructuredPropertyAction action = new UnsetStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(VALID_PARAMS);

    // Mock service to throw exception
    doThrow(new RuntimeException("Service error"))
        .when(service)
        .batchUnsetStructuredProperty(any(), any(), any(), any());

    action.apply(mock(OperationContext.class), DATASET_URNS, params);
  }

  @Test
  public void testValuesActionBehavior() throws InvalidOperandException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    UnsetStructuredPropertyAction action = new UnsetStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(VALID_PARAMS);

    // Should not throw validation errors for valid parameters
    action.apply(mock(OperationContext.class), DATASET_URNS, params);
  }
}
