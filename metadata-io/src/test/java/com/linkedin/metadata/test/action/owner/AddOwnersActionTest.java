package com.linkedin.metadata.test.action.owner;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.service.OwnerServiceAsync;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AddOwnersActionTest {

  private static final List<Urn> TEST_OWNERS =
      ImmutableList.of(
          UrnUtils.getUrn("urn:li:corpuser:test"), UrnUtils.getUrn("urn:li:corpuser:test2"));

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

  private static final List<ResourceReference> DATASET_REFERENCES =
      DATASET_URNS.stream()
          .map(urn -> new ResourceReference(urn, null, null))
          .collect(Collectors.toList());

  private static final List<ResourceReference> DASHBOARD_REFERENCES =
      DASHBOARD_URNS.stream()
          .map(urn -> new ResourceReference(urn, null, null))
          .collect(Collectors.toList());

  private static final Map<String, List<String>> VALID_PARAMS =
      ImmutableMap.of(
          "values", TEST_OWNERS.stream().map(Urn::toString).collect(Collectors.toList()),
          "ownerType", ImmutableList.of(OwnershipType.TECHNICAL_OWNER.toString()));

  @Test
  private void testApply() throws Exception {
    OwnerServiceAsync service = mock(OwnerServiceAsync.class);

    AddOwnersAction action = new AddOwnersAction(service);
    ActionParameters params = new ActionParameters(VALID_PARAMS);
    action.apply(mock(OperationContext.class), ALL_URNS, params);

    Mockito.verify(service, Mockito.atLeastOnce())
        .batchAddOwners(
            any(OperationContext.class),
            Mockito.eq(TEST_OWNERS),
            Mockito.eq(DASHBOARD_REFERENCES),
            Mockito.eq(OwnershipType.TECHNICAL_OWNER),
            Mockito.eq(null),
            Mockito.eq(METADATA_TESTS_SOURCE),
            Mockito.eq(null));

    Mockito.verify(service, Mockito.atLeastOnce())
        .batchAddOwners(
            any(OperationContext.class),
            Mockito.eq(TEST_OWNERS),
            Mockito.eq(DATASET_REFERENCES),
            Mockito.eq(OwnershipType.TECHNICAL_OWNER),
            Mockito.eq(null),
            Mockito.eq(METADATA_TESTS_SOURCE),
            Mockito.eq(null));

    Mockito.verifyNoMoreInteractions(service);
  }

  @Test
  private void testValidateValidParams() {
    AddOwnersAction action = new AddOwnersAction(mock(OwnerServiceAsync.class));
    ActionParameters params = new ActionParameters(VALID_PARAMS);
    action.validate(params);
  }

  @Test
  private void testValidateInvalidParams() {
    AddOwnersAction action = new AddOwnersAction(mock(OwnerServiceAsync.class));
    ActionParameters params = new ActionParameters(Collections.emptyMap());
    Assert.assertThrows(InvalidActionParamsException.class, () -> action.validate(params));
  }
}
