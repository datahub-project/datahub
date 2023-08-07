package com.linkedin.metadata.test.action.domain;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.service.DomainService;
import com.linkedin.metadata.test.action.ActionParameters;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.*;


public class UnsetDomainActionTest {

  private static final Urn TEST_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:test");

  private static final List<Urn> DATASET_URNS = ImmutableList.of(
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test,PROD)"),
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test1,PROD)")
  );

  private static final List<Urn> DASHBOARD_URNS = ImmutableList.of(
      UrnUtils.getUrn("urn:li:dashboard:(looker,1)"),
      UrnUtils.getUrn("urn:li:dashboard:(looker,2)")
  );

  private static final Map<String, List<String>> VALID_PARAMS = ImmutableMap.of(
          "values",
          ImmutableList.of(TEST_DOMAIN_URN.toString())
  );

  private static final List<Urn> ALL_URNS = new ArrayList<>();

  static {
    ALL_URNS.addAll(DATASET_URNS);
    ALL_URNS.addAll(DASHBOARD_URNS);
  }

  private static final List<ResourceReference> DATASET_REFERENCES = DATASET_URNS.stream().map(
      urn -> new ResourceReference(urn, null, null)
  ).collect(Collectors.toList());

  private static final List<ResourceReference> DASHBOARD_REFERENCES = DASHBOARD_URNS.stream().map(
      urn -> new ResourceReference(urn, null, null)
  ).collect(Collectors.toList());

  @Test
  private void testApply() throws Exception {
    DomainService service = Mockito.mock(DomainService.class);

    UnsetDomainAction action = new UnsetDomainAction(service);
    ActionParameters params = new ActionParameters(VALID_PARAMS);
    action.apply(ALL_URNS, params);

    Mockito.verify(service, Mockito.atLeastOnce()).batchRemoveDomains(
        Mockito.eq(ImmutableList.of(TEST_DOMAIN_URN)),
        Mockito.eq(DASHBOARD_REFERENCES),
        Mockito.eq(METADATA_TESTS_SOURCE)
    );
    Mockito.verify(service, Mockito.atLeastOnce()).batchRemoveDomains(
        Mockito.eq(ImmutableList.of(TEST_DOMAIN_URN)),
        Mockito.eq(DATASET_REFERENCES),
        Mockito.eq(METADATA_TESTS_SOURCE)
    );

    Mockito.verifyNoMoreInteractions(service);
  }

  @Test
  private void testValidateValidParams() {
    UnsetDomainAction action = new UnsetDomainAction(Mockito.mock(DomainService.class));
    ActionParameters params = new ActionParameters(VALID_PARAMS);
    action.validate(params);
  }
}