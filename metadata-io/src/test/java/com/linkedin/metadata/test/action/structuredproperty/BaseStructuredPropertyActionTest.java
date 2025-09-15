package com.linkedin.metadata.test.action.structuredproperty;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.mock;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.service.StructuredPropertyService;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.definition.ActionType;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BaseStructuredPropertyActionTest {

  private static final List<Urn> DATASET_URNS =
      ImmutableList.of(
          UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test,PROD)"),
          UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test1,PROD)"));

  private static final List<Urn> DASHBOARD_URNS =
      ImmutableList.of(
          UrnUtils.getUrn("urn:li:chart:(looker,dashboard.chart1)"),
          UrnUtils.getUrn("urn:li:dashboard:(looker,dashboard1)"));

  private static final List<Urn> ALL_URNS = new ArrayList<>();

  static {
    ALL_URNS.addAll(DATASET_URNS);
    ALL_URNS.addAll(DASHBOARD_URNS);
  }

  private static final Map<String, List<String>> VALID_SINGLE_PROPERTY_PARAMS =
      ImmutableMap.of("values", ImmutableList.of("urn:li:structuredProperty:test.property"));

  private static final Map<String, List<String>> INVALID_MULTIPLE_PROPERTY_PARAMS =
      ImmutableMap.of(
          "values",
          ImmutableList.of(
              "urn:li:structuredProperty:test.property1",
              "urn:li:structuredProperty:test.property2"));

  private static final Map<String, List<String>> PARAMS_WITH_STRING_VALUES =
      ImmutableMap.of(
          "values", ImmutableList.of("urn:li:structuredProperty:test.property"),
          "stringValues", ImmutableList.of("value1", "value2"));

  private static final Map<String, List<String>> PARAMS_WITH_NUMBER_VALUES =
      ImmutableMap.of(
          "values", ImmutableList.of("urn:li:structuredProperty:test.property"),
          "numberValues", ImmutableList.of("123.45", "678.90"));

  // Test implementation of the abstract class
  private static class TestStructuredPropertyAction extends BaseStructuredPropertyAction {
    public TestStructuredPropertyAction(StructuredPropertyService structuredPropertyService) {
      super(structuredPropertyService);
    }

    @Override
    public ActionType getActionType() {
      return ActionType.SET_STRUCTURED_PROPERTY; // Just for testing
    }

    @Override
    void applyInternal(
        @Nonnull OperationContext opContext,
        Urn structuredPropertyUrn,
        List<Urn> urns,
        ActionParameters params) {
      // Test implementation - just verify the method is called
    }
  }

  @Test
  public void testValidValueEntityTypes() {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    TestStructuredPropertyAction action = new TestStructuredPropertyAction(service);

    Assert.assertTrue(action.validValueEntityTypes().contains("structuredProperty"));
  }

  @Test
  public void testValidationPassesWithSingleProperty() throws InvalidActionParamsException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    TestStructuredPropertyAction action = new TestStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(VALID_SINGLE_PROPERTY_PARAMS);

    // Should pass validation with single structured property
    action.validate(params);
  }

  @Test(expectedExceptions = InvalidActionParamsException.class)
  public void testValidationFailsWithMultipleProperties() throws InvalidActionParamsException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    TestStructuredPropertyAction action = new TestStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(INVALID_MULTIPLE_PROPERTY_PARAMS);

    // Should throw exception when multiple structured properties provided
    action.validate(params);
  }

  @Test(expectedExceptions = InvalidActionParamsException.class)
  public void testValidateMissingValues() throws InvalidActionParamsException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    TestStructuredPropertyAction action = new TestStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(ImmutableMap.of());

    action.validate(params);
  }

  @Test(expectedExceptions = InvalidActionParamsException.class)
  public void testValidateEmptyValues() throws InvalidActionParamsException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    TestStructuredPropertyAction action = new TestStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(ImmutableMap.of("values", ImmutableList.of()));

    action.validate(params);
  }

  @Test
  public void testApplyWithSingleStructuredProperty() throws InvalidOperandException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    TestStructuredPropertyAction action = new TestStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(VALID_SINGLE_PROPERTY_PARAMS);

    // Should process the single structured property
    action.apply(mock(OperationContext.class), DATASET_URNS, params);
  }

  @Test
  public void testApplyWithStringValues() throws InvalidOperandException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    TestStructuredPropertyAction action = new TestStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(PARAMS_WITH_STRING_VALUES);

    action.apply(mock(OperationContext.class), DATASET_URNS, params);
  }

  @Test
  public void testApplyWithNumberValues() throws InvalidOperandException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    TestStructuredPropertyAction action = new TestStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(PARAMS_WITH_NUMBER_VALUES);

    action.apply(mock(OperationContext.class), DATASET_URNS, params);
  }

  @Test
  public void testApplyWithMixedEntityTypes() throws InvalidOperandException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    TestStructuredPropertyAction action = new TestStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(PARAMS_WITH_STRING_VALUES);

    // Should group entities by type and apply the structured property to each group
    action.apply(mock(OperationContext.class), ALL_URNS, params);
  }

  @Test
  public void testApplyWithEmptyUrns() throws InvalidOperandException {
    StructuredPropertyService service = mock(StructuredPropertyService.class);
    TestStructuredPropertyAction action = new TestStructuredPropertyAction(service);
    ActionParameters params = new ActionParameters(VALID_SINGLE_PROPERTY_PARAMS);

    // Should not throw exception for empty URNs
    action.apply(mock(OperationContext.class), ImmutableList.of(), params);
  }
}
