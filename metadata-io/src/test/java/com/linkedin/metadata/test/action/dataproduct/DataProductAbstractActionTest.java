package com.linkedin.metadata.test.action.dataproduct;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.mock;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.service.DataProductService;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.definition.ActionType;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DataProductAbstractActionTest {

  private static final Urn TEST_DATA_PRODUCT_URN = UrnUtils.getUrn("urn:li:dataProduct:test");
  private static final Urn INVALID_URN = UrnUtils.getUrn("urn:li:tag:test"); // Not a data product

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

  private static final Map<String, List<String>> INVALID_PARAMS =
      ImmutableMap.of("values", ImmutableList.of(INVALID_URN.toString()));

  // Test implementation of DataProductAbstractAction
  private static class TestDataProductAction extends DataProductAbstractAction {
    private boolean applyInternalCalled = false;
    private Urn receivedDataProductUrn = null;
    private List<Urn> receivedUrns = null;

    public TestDataProductAction(DataProductService dataProductService) {
      super(dataProductService);
    }

    @Override
    public ActionType getActionType() {
      return ActionType.SET_DATA_PRODUCT; // Just for testing
    }

    @Override
    void applyInternal(@Nonnull OperationContext opContext, Urn dataProductUrn, List<Urn> urns) {
      this.applyInternalCalled = true;
      this.receivedDataProductUrn = dataProductUrn;
      this.receivedUrns = new ArrayList<>(urns);
    }

    public boolean wasApplyInternalCalled() {
      return applyInternalCalled;
    }

    public Urn getReceivedDataProductUrn() {
      return receivedDataProductUrn;
    }

    public List<Urn> getReceivedUrns() {
      return receivedUrns;
    }
  }

  @Test
  public void testApplyGroupsEntitiesByType() throws Exception {
    DataProductService service = mock(DataProductService.class);
    TestDataProductAction action = new TestDataProductAction(service);
    ActionParameters params = new ActionParameters(VALID_PARAMS);

    action.apply(mock(OperationContext.class), ALL_URNS, params);

    // Verify applyInternal was called (should be called once per entity type group)
    Assert.assertTrue(action.wasApplyInternalCalled());
    Assert.assertEquals(action.getReceivedDataProductUrn(), TEST_DATA_PRODUCT_URN);

    // Should have received some URNs (either datasets or dashboards)
    Assert.assertNotNull(action.getReceivedUrns());
    Assert.assertFalse(action.getReceivedUrns().isEmpty());
  }

  @Test
  public void testValidateValidParams() {
    DataProductService service = mock(DataProductService.class);
    TestDataProductAction action = new TestDataProductAction(service);
    ActionParameters params = new ActionParameters(VALID_PARAMS);

    // Should not throw exception
    action.validate(params);
  }

  @Test
  public void testValidateInvalidParams() {
    DataProductService service = mock(DataProductService.class);
    TestDataProductAction action = new TestDataProductAction(service);
    ActionParameters params = new ActionParameters(Collections.emptyMap());

    // Should throw exception for missing values
    Assert.assertThrows(InvalidActionParamsException.class, () -> action.validate(params));
  }

  @Test
  public void testValidateInvalidEntityType() {
    DataProductService service = mock(DataProductService.class);
    TestDataProductAction action = new TestDataProductAction(service);
    ActionParameters params = new ActionParameters(INVALID_PARAMS);

    // Should throw exception for invalid entity type (tag instead of data product)
    Assert.assertThrows(InvalidActionParamsException.class, () -> action.validate(params));
  }

  @Test
  public void testApplyEmptyUrns() throws Exception {
    DataProductService service = mock(DataProductService.class);
    TestDataProductAction action = new TestDataProductAction(service);
    ActionParameters params = new ActionParameters(VALID_PARAMS);

    // Apply with empty URN list
    action.apply(mock(OperationContext.class), Collections.emptyList(), params);

    // applyInternal should not be called since no URNs to process
    Assert.assertFalse(action.wasApplyInternalCalled());
  }

  @Test
  public void testValidValueEntityTypes() {
    DataProductService service = mock(DataProductService.class);
    TestDataProductAction action = new TestDataProductAction(service);

    // Should only accept DATA_PRODUCT_ENTITY_NAME
    Assert.assertEquals(action.validValueEntityTypes().size(), 1);
    Assert.assertTrue(action.validValueEntityTypes().contains(DATA_PRODUCT_ENTITY_NAME));
  }
}
