package com.linkedin.metadata.test.action.structuredproperty;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

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
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SetStructuredPropertyActionTest {

  @Mock private StructuredPropertyService mockStructuredPropertyService;
  @Mock private OperationContext mockOpContext;

  private SetStructuredPropertyAction action;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    action = new SetStructuredPropertyAction(mockStructuredPropertyService);
  }

  @Test
  public void testGetActionType() {
    assertEquals(ActionType.SET_STRUCTURED_PROPERTY, action.getActionType());
  }

  @Test
  public void testValidateValidStringParams() {
    ActionParameters params =
        new ActionParameters(
            Map.of(
                "values", List.of("urn:li:structuredProperty:test.property"),
                "stringValues", List.of("test value", "another value")));

    assertDoesNotThrow(() -> action.validate(params));
  }

  @Test
  public void testValidateValidNumberParams() {
    ActionParameters params =
        new ActionParameters(
            Map.of(
                "values", List.of("urn:li:structuredProperty:test.property"),
                "numberValues", List.of("123.45", "67.89")));

    assertDoesNotThrow(() -> action.validate(params));
  }

  @Test
  public void testValidateMissingValueParams() {
    ActionParameters params =
        new ActionParameters(Map.of("values", List.of("urn:li:structuredProperty:test.property")));

    assertThrows(InvalidActionParamsException.class, () -> action.validate(params));
  }

  @Test
  public void testValidateBothStringAndNumberParams() {
    ActionParameters params =
        new ActionParameters(
            Map.of(
                "values", List.of("urn:li:structuredProperty:test.property"),
                "stringValues", List.of("test value"),
                "numberValues", List.of("123.45")));

    assertThrows(InvalidActionParamsException.class, () -> action.validate(params));
  }

  @Test
  public void testValidateEmptyStringValues() {
    ActionParameters params =
        new ActionParameters(
            Map.of(
                "values", List.of("urn:li:structuredProperty:test.property"),
                "stringValues", List.of()));

    assertThrows(InvalidActionParamsException.class, () -> action.validate(params));
  }

  @Test
  public void testApplyWithStringValues() throws InvalidOperandException {
    List<Urn> urns =
        List.of(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test.topic,PROD)"));
    ActionParameters params =
        new ActionParameters(
            Map.of(
                "values", List.of("urn:li:structuredProperty:test.property"),
                "stringValues", List.of("test value")));

    action.apply(mockOpContext, urns, params);

    // Verify that batchSetStructuredProperty was called with correct parameters
    ArgumentCaptor<Urn> structuredPropertyUrnCaptor = ArgumentCaptor.forClass(Urn.class);
    ArgumentCaptor<List<ResourceReference>> resourcesCaptor = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<List<StructuredPropertyValueAssignment>> assignmentsCaptor =
        ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<String> appSourceCaptor = ArgumentCaptor.forClass(String.class);

    verify(mockStructuredPropertyService)
        .batchSetStructuredProperty(
            eq(mockOpContext),
            structuredPropertyUrnCaptor.capture(),
            resourcesCaptor.capture(),
            assignmentsCaptor.capture(),
            appSourceCaptor.capture());

    assertEquals(
        "urn:li:structuredProperty:test.property",
        structuredPropertyUrnCaptor.getValue().toString());
    assertEquals(1, resourcesCaptor.getValue().size());
    assertEquals(urns.get(0), resourcesCaptor.getValue().get(0).getUrn());
    assertEquals(1, assignmentsCaptor.getValue().size());
    assertEquals("METADATA_TESTS", appSourceCaptor.getValue());
  }

  @Test
  public void testApplyWithNumberValues() throws InvalidOperandException {
    List<Urn> urns =
        List.of(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test.topic,PROD)"));
    ActionParameters params =
        new ActionParameters(
            Map.of(
                "values", List.of("urn:li:structuredProperty:test.property"),
                "numberValues", List.of("123.45")));

    action.apply(mockOpContext, urns, params);

    // Verify that batchSetStructuredProperty was called
    verify(mockStructuredPropertyService)
        .batchSetStructuredProperty(
            eq(mockOpContext),
            any(Urn.class),
            any(List.class),
            any(List.class),
            eq("METADATA_TESTS"));
  }

  @Test
  public void testApplyWithMultipleUrns() throws InvalidOperandException {
    List<Urn> urns =
        List.of(
            UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test.topic1,PROD)"),
            UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test.topic2,PROD)"));
    ActionParameters params =
        new ActionParameters(
            Map.of(
                "values", List.of("urn:li:structuredProperty:test.property"),
                "stringValues", List.of("test value")));

    action.apply(mockOpContext, urns, params);

    // Verify that batchSetStructuredProperty was called with all URNs
    ArgumentCaptor<List<ResourceReference>> resourcesCaptor = ArgumentCaptor.forClass(List.class);
    verify(mockStructuredPropertyService)
        .batchSetStructuredProperty(
            eq(mockOpContext),
            any(Urn.class),
            resourcesCaptor.capture(),
            any(List.class),
            eq("METADATA_TESTS"));

    assertEquals(2, resourcesCaptor.getValue().size());
  }

  @Test
  public void testApplyWithEmptyUrns() throws InvalidOperandException {
    List<Urn> urns = List.of();
    ActionParameters params =
        new ActionParameters(
            Map.of(
                "values", List.of("urn:li:structuredProperty:test.property"),
                "stringValues", List.of("test value")));

    action.apply(mockOpContext, urns, params);

    // Should not call the service method for empty URNs
    verify(mockStructuredPropertyService, never())
        .batchSetStructuredProperty(any(), any(), any(), any(), any());
  }
}
