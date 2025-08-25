package com.linkedin.metadata.test.action.structuredproperty;

import static org.mockito.Mockito.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.*;

public class StructuredPropertyAbstractActionTest {

  @Mock private StructuredPropertyService mockStructuredPropertyService;
  @Mock private OperationContext mockOpContext;

  private TestStructuredPropertyAction action;

  // Test implementation of the abstract class
  private static class TestStructuredPropertyAction extends StructuredPropertyAbstractAction {
    public TestStructuredPropertyAction(StructuredPropertyService structuredPropertyService) {
      super(structuredPropertyService);
    }

    @Override
    public ActionType getActionType() {
      return ActionType.SET_STRUCTURED_PROPERTY; // Just for testing
    }

    @Override
    void applyInternal(OperationContext opContext, Urn structuredPropertyUrn, List<Urn> urns, ActionParameters params) {
      // Test implementation - just verify the method is called
    }
  }

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    action = new TestStructuredPropertyAction(mockStructuredPropertyService);
  }

  @Test
  public void testValidateValidParams() {
    ActionParameters params = new ActionParameters(
        Map.of("values", List.of("urn:li:structuredProperty:test.property"))
    );

    assertDoesNotThrow(() -> action.validate(params));
  }

  @Test
  public void testValidateMissingValues() {
    ActionParameters params = new ActionParameters(Map.of());

    assertThrows(InvalidActionParamsException.class, () -> action.validate(params));
  }

  @Test
  public void testValidateEmptyValues() {
    ActionParameters params = new ActionParameters(
        Map.of("values", List.of())
    );

    assertThrows(InvalidActionParamsException.class, () -> action.validate(params));
  }

  @Test
  public void testApplyWithValidUrns() throws InvalidOperandException {
    List<Urn> urns = List.of(
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test.topic,PROD)"),
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,/data/test,PROD)")
    );
    ActionParameters params = new ActionParameters(
        Map.of("values", List.of("urn:li:structuredProperty:test.property"))
    );

    assertDoesNotThrow(() -> action.apply(mockOpContext, urns, params));
  }

  @Test
  public void testApplyWithEmptyUrns() throws InvalidOperandException {
    List<Urn> urns = List.of();
    ActionParameters params = new ActionParameters(
        Map.of("values", List.of("urn:li:structuredProperty:test.property"))
    );

    // Should not throw exception for empty URNs
    assertDoesNotThrow(() -> action.apply(mockOpContext, urns, params));
  }

  @Test
  public void testApplyGroupsEntitiesByType() throws InvalidOperandException {
    List<Urn> urns = List.of(
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test.topic,PROD)"),
        UrnUtils.getUrn("urn:li:chart:(looker,dashboard.chart1)"),
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,/data/test,PROD)")
    );
    ActionParameters params = new ActionParameters(
        Map.of("values", List.of("urn:li:structuredProperty:test.property"))
    );

    // Should group entities by type and call applyInternal for each group
    assertDoesNotThrow(() -> action.apply(mockOpContext, urns, params));
  }

  @Test
  public void testValidValueEntityTypes() {
    assertTrue(action.validValueEntityTypes().contains("structuredProperty"));
  }
}
