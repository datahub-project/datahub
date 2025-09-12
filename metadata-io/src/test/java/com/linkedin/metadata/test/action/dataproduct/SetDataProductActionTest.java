package com.linkedin.metadata.test.action.dataproduct;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.service.DataProductService;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SetDataProductActionTest {

  private static final Urn TEST_DATA_PRODUCT_URN = UrnUtils.getUrn("urn:li:dataProduct:test");

  private static final List<Urn> DATASET_URNS =
      ImmutableList.of(
          UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test,PROD)"),
          UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test1,PROD)"));

  private static final List<Urn> DASHBOARD_URNS =
      ImmutableList.of(
          UrnUtils.getUrn("urn:li:dashboard:(looker,1)"),
          UrnUtils.getUrn("urn:li:dashboard:(looker,2)"));

  private static final List<Urn> ALL_URNS = new ArrayList<>();

  static {
    ALL_URNS.addAll(DATASET_URNS);
    ALL_URNS.addAll(DASHBOARD_URNS);
  }

  private static final Map<String, List<String>> VALID_PARAMS =
      ImmutableMap.of("values", ImmutableList.of(TEST_DATA_PRODUCT_URN.toString()));

  @Test
  public void testApply() throws Exception {
    DataProductService service = mock(DataProductService.class);

    SetDataProductAction action = new SetDataProductAction(service);
    ActionParameters params = new ActionParameters(VALID_PARAMS);
    action.apply(mock(OperationContext.class), ALL_URNS, params);

    // Verify that batchSetDataProduct is called (parameters will be ResourceReference now)
    Mockito.verify(service, Mockito.atLeastOnce())
        .batchSetDataProduct(
            any(OperationContext.class),
            Mockito.eq(TEST_DATA_PRODUCT_URN),
            any(), // ResourceReference list
            any()); // appSource

    Mockito.verifyNoMoreInteractions(service);
  }

  @Test
  public void testValidateValidParams() {
    SetDataProductAction action = new SetDataProductAction(mock(DataProductService.class));
    ActionParameters params = new ActionParameters(VALID_PARAMS);
    action.validate(params);
  }

  @Test
  public void testValidateInvalidParams() {
    SetDataProductAction action = new SetDataProductAction(mock(DataProductService.class));
    ActionParameters params = new ActionParameters(Collections.emptyMap());
    Assert.assertThrows(InvalidActionParamsException.class, () -> action.validate(params));
  }

  @Test
  public void testValidateMultipleDataProductUrns() {
    SetDataProductAction action = new SetDataProductAction(mock(DataProductService.class));
    Map<String, List<String>> multipleParams =
        ImmutableMap.of(
            "values", ImmutableList.of("urn:li:dataProduct:test1", "urn:li:dataProduct:test2"));
    ActionParameters params = new ActionParameters(multipleParams);

    // SetDataProductAction should only accept one data product URN
    Assert.assertThrows(InvalidActionParamsException.class, () -> action.validate(params));
  }

  @Test
  public void testApplyEmptyUrns() throws Exception {
    DataProductService service = mock(DataProductService.class);
    SetDataProductAction action = new SetDataProductAction(service);
    ActionParameters params = new ActionParameters(VALID_PARAMS);

    // Apply with empty URN list should not call service
    action.apply(mock(OperationContext.class), Collections.emptyList(), params);

    Mockito.verifyNoInteractions(service);
  }
}
