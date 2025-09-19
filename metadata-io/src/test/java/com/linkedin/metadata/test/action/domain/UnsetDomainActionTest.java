package com.linkedin.metadata.test.action.domain;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.service.DomainServiceAsync;
import com.linkedin.metadata.test.action.ActionParameters;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

public class UnsetDomainActionTest {

  private static final List<Urn> DATASET_URNS =
      ImmutableList.of(
          UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test,PROD)"),
          UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test1,PROD)"));

  private static final List<Urn> DASHBOARD_URNS =
      ImmutableList.of(
          UrnUtils.getUrn("urn:li:dashboard:(looker,1)"),
          UrnUtils.getUrn("urn:li:dashboard:(looker,2)"));

  // UnsetDomainAction doesn't require parameters since it removes ALL domains
  private static final Map<String, List<String>> EMPTY_PARAMS = Collections.emptyMap();

  private static final List<Urn> ALL_URNS = new ArrayList<>();

  static {
    ALL_URNS.addAll(DATASET_URNS);
    ALL_URNS.addAll(DASHBOARD_URNS);
  }

  private static final List<ResourceReference> ALL_REFERENCES =
      ALL_URNS.stream()
          .map(urn -> new ResourceReference(urn, null, null))
          .collect(Collectors.toList());

  @Test
  public void testApply() throws Exception {
    DomainServiceAsync service = mock(DomainServiceAsync.class);

    UnsetDomainAction action = new UnsetDomainAction(service);
    ActionParameters params = new ActionParameters(EMPTY_PARAMS);
    action.apply(mock(OperationContext.class), ALL_URNS, params);

    // Verify that batchUnsetDomain is called once with all URNs (removes ALL domains)
    Mockito.verify(service, Mockito.times(1))
        .batchUnsetDomain(
            any(OperationContext.class),
            Mockito.eq(ALL_REFERENCES),
            Mockito.eq(METADATA_TESTS_SOURCE));

    Mockito.verifyNoMoreInteractions(service);
  }

  @Test
  public void testApplyEmptyUrns() throws Exception {
    DomainServiceAsync service = mock(DomainServiceAsync.class);
    UnsetDomainAction action = new UnsetDomainAction(service);
    ActionParameters params = new ActionParameters(EMPTY_PARAMS);

    // Apply with empty URN list should not call service
    action.apply(mock(OperationContext.class), Collections.emptyList(), params);

    Mockito.verifyNoInteractions(service);
  }

  @Test
  public void testValidateNoValidation() {
    UnsetDomainAction action = new UnsetDomainAction(mock(DomainServiceAsync.class));
    ActionParameters params = new ActionParameters(EMPTY_PARAMS);

    // UnsetDomainAction extends NoValidationAction, so validation should pass
    action.validate(params);
  }

  @Test
  public void testValidateWithParams() {
    UnsetDomainAction action = new UnsetDomainAction(mock(DomainServiceAsync.class));
    Map<String, List<String>> paramsWithValues =
        Collections.singletonMap("values", ImmutableList.of("urn:li:domain:test"));
    ActionParameters params = new ActionParameters(paramsWithValues);

    // Should still pass validation even with parameters since NoValidationAction doesn't validate
    action.validate(params);
  }

  @Test
  public void testBackwardCompatibilityWithDomainUrns() throws Exception {
    DomainServiceAsync service = mock(DomainServiceAsync.class);
    UnsetDomainAction action = new UnsetDomainAction(service);

    // Test backward compatibility: in the old version, users would specify domain URNs to remove
    // In the new version, we ignore these parameters and remove ALL domains
    Map<String, List<String>> oldStyleParams =
        Collections.singletonMap(
            "values", ImmutableList.of("urn:li:domain:engineering", "urn:li:domain:marketing"));
    ActionParameters params = new ActionParameters(oldStyleParams);

    action.apply(mock(OperationContext.class), ALL_URNS, params);

    // Verify that batchUnsetDomain is called (removes ALL domains, ignoring the specific domain
    // URNs)
    Mockito.verify(service, Mockito.times(1))
        .batchUnsetDomain(
            any(OperationContext.class),
            Mockito.eq(ALL_REFERENCES),
            Mockito.eq(METADATA_TESTS_SOURCE));

    Mockito.verifyNoMoreInteractions(service);
  }
}
