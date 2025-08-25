package com.linkedin.metadata.test.action.structuredproperty;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.service.StructuredPropertyService;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.definition.ActionType;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class UnsetStructuredPropertyActionTest {

  @Mock private StructuredPropertyService mockStructuredPropertyService;
  @Mock private OperationContext mockOpContext;

  private UnsetStructuredPropertyAction action;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    action = new UnsetStructuredPropertyAction(mockStructuredPropertyService);
  }

  @Test
  public void testGetActionType() {
    assertEquals(ActionType.UNSET_STRUCTURED_PROPERTY, action.getActionType());
  }

  @Test
  public void testValidateValidParams() {
    ActionParameters params = new ActionParameters(
        Map.of("structuredPropertyUrn", List.of("urn:li:structuredProperty:test.property"))
    );

    assertDoesNotThrow(() -> action.validate(params));
  }

  @Test
  public void testValidateMissingStructuredPropertyUrn() {
    ActionParameters params = new ActionParameters(Map.of());

    assertThrows(InvalidActionParamsException.class, () -> action.validate(params));
  }

  @Test
  public void testValidateEmptyStructuredPropertyUrn() {
    ActionParameters params = new ActionParameters(
        Map.of("structuredPropertyUrn", List.of())
    );

    assertThrows(InvalidActionParamsException.class, () -> action.validate(params));
  }

  @Test
  public void testApply() throws InvalidOperandException {
    List<Urn> urns = List.of(
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test.topic,PROD)")
    );
    ActionParameters params = new ActionParameters(
        Map.of("structuredPropertyUrn", List.of("urn:li:structuredProperty:test.property"))
    );

    action.apply(mockOpContext, urns, params);

    // Verify that batchUnsetStructuredProperty was called with correct parameters
    ArgumentCaptor<Urn> structuredPropertyUrnCaptor = ArgumentCaptor.forClass(Urn.class);
    ArgumentCaptor<List<ResourceReference>> resourcesCaptor = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<String> appSourceCaptor = ArgumentCaptor.forClass(String.class);

    verify(mockStructuredPropertyService).batchUnsetStructuredProperty(
        eq(mockOpContext),
        structuredPropertyUrnCaptor.capture(),
        resourcesCaptor.capture(),
        appSourceCaptor.capture()
    );

    assertEquals("urn:li:structuredProperty:test.property", structuredPropertyUrnCaptor.getValue().toString());
    assertEquals(1, resourcesCaptor.getValue().size());
    assertEquals(urns.get(0), resourcesCaptor.getValue().get(0).getUrn());
    assertEquals("METADATA_TESTS", appSourceCaptor.getValue());
  }

  @Test
  public void testApplyWithMultipleUrns() throws InvalidOperandException {
    List<Urn> urns = List.of(
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test.topic1,PROD)"),
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test.topic2,PROD)")
    );
    ActionParameters params = new ActionParameters(
        Map.of("structuredPropertyUrn", List.of("urn:li:structuredProperty:test.property"))
    );

    action.apply(mockOpContext, urns, params);

    // Verify that batchUnsetStructuredProperty was called with all URNs
    ArgumentCaptor<List<ResourceReference>> resourcesCaptor = ArgumentCaptor.forClass(List.class);
    verify(mockStructuredPropertyService).batchUnsetStructuredProperty(
        eq(mockOpContext),
        any(Urn.class),
        resourcesCaptor.capture(),
        eq("METADATA_TESTS")
    );

    assertEquals(2, resourcesCaptor.getValue().size());
  }

  @Test
  public void testApplyWithEmptyUrns() throws InvalidOperandException {
    List<Urn> urns = List.of();
    ActionParameters params = new ActionParameters(
        Map.of("structuredPropertyUrn", List.of("urn:li:structuredProperty:test.property"))
    );

    action.apply(mockOpContext, urns, params);

    // Should not call the service method for empty URNs
    verify(mockStructuredPropertyService, never()).batchUnsetStructuredProperty(
        any(), any(), any(), any()
    );
  }

  @Test
  public void testApplyHandlesServiceException() {
    List<Urn> urns = List.of(
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test.topic,PROD)")
    );
    ActionParameters params = new ActionParameters(
        Map.of("structuredPropertyUrn", List.of("urn:li:structuredProperty:test.property"))
    );

    // Mock service to throw exception
    doThrow(new RuntimeException("Service error")).when(mockStructuredPropertyService)
        .batchUnsetStructuredProperty(any(), any(), any(), any());

    assertThrows(InvalidOperandException.class, () -> action.apply(mockOpContext, urns, params));
  }

  @Test
  public void testNoValidationActionBehavior() {
    // UnsetStructuredPropertyAction extends NoValidationAction, so it should not validate URN values
    // This test ensures the action follows NoValidationAction pattern
    List<Urn> urns = List.of(
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test.topic,PROD)")
    );
    ActionParameters params = new ActionParameters(
        Map.of("structuredPropertyUrn", List.of("urn:li:structuredProperty:test.property"))
    );

    // Should not throw validation errors for URN values since it extends NoValidationAction
    assertDoesNotThrow(() -> action.apply(mockOpContext, urns, params));
  }
}
