package com.linkedin.metadata.test.action.dataproduct;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.service.DataProductService;
import com.linkedin.metadata.test.action.ActionParameters;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

public class UnsetDataProductActionTest {

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

  // UnsetDataProductAction doesn't require parameters since it removes ALL data products from
  // entities
  private static final Map<String, List<String>> EMPTY_PARAMS = Collections.emptyMap();

  @Test
  public void testApply() throws Exception {
    DataProductService service = mock(DataProductService.class);

    UnsetDataProductAction action = new UnsetDataProductAction(service);
    ActionParameters params = new ActionParameters(EMPTY_PARAMS);
    action.apply(mock(OperationContext.class), ALL_URNS, params);

    // Verify that batchUnsetDataProduct is called once (parameters will be ResourceReference now)
    Mockito.verify(service, Mockito.times(1))
        .batchUnsetDataProduct(any(OperationContext.class), any(), any());

    Mockito.verifyNoMoreInteractions(service);
  }

  @Test
  public void testApplyEmptyUrns() throws Exception {
    DataProductService service = mock(DataProductService.class);
    UnsetDataProductAction action = new UnsetDataProductAction(service);
    ActionParameters params = new ActionParameters(EMPTY_PARAMS);

    // Apply with empty URN list should not call service
    action.apply(mock(OperationContext.class), Collections.emptyList(), params);

    Mockito.verifyNoInteractions(service);
  }

  @Test
  public void testValidateNoValidation() {
    UnsetDataProductAction action = new UnsetDataProductAction(mock(DataProductService.class));
    ActionParameters params = new ActionParameters(EMPTY_PARAMS);

    // UnsetDataProductAction extends NoValidationAction, so validation should pass
    action.validate(params);
  }

  @Test
  public void testValidateWithParams() {
    UnsetDataProductAction action = new UnsetDataProductAction(mock(DataProductService.class));
    Map<String, List<String>> paramsWithValues =
        ImmutableMap.of("values", ImmutableList.of("urn:li:dataProduct:test"));
    ActionParameters params = new ActionParameters(paramsWithValues);

    // Should still pass validation even with parameters since NoValidationAction doesn't validate
    action.validate(params);
  }
}
