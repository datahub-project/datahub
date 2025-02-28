package com.linkedin.metadata.test.action.term;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.service.GlossaryTermServiceAsync;
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

public class AddGlossaryTermsActionTest {

  private static final List<Urn> TEST_TERMS =
      ImmutableList.of(
          UrnUtils.getUrn("urn:li:glossaryTerm:test"),
          UrnUtils.getUrn("urn:li:glossaryTerm:test2"));

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
          "values", TEST_TERMS.stream().map(Urn::toString).collect(Collectors.toList()));

  @Test
  private void testApply() throws Exception {
    GlossaryTermServiceAsync service = mock(GlossaryTermServiceAsync.class);

    AddGlossaryTermsAction action = new AddGlossaryTermsAction(service);
    ActionParameters params = new ActionParameters(VALID_PARAMS);
    action.apply(mock(OperationContext.class), ALL_URNS, params);

    Mockito.verify(service, Mockito.atLeastOnce())
        .batchAddGlossaryTerms(
            any(OperationContext.class),
            Mockito.eq(TEST_TERMS),
            Mockito.eq(DASHBOARD_REFERENCES),
            Mockito.eq(METADATA_TESTS_SOURCE),
            Mockito.eq(null));

    Mockito.verify(service, Mockito.atLeastOnce())
        .batchAddGlossaryTerms(
            any(OperationContext.class),
            Mockito.eq(TEST_TERMS),
            Mockito.eq(DATASET_REFERENCES),
            Mockito.eq(METADATA_TESTS_SOURCE),
            Mockito.eq(null));

    Mockito.verifyNoMoreInteractions(service);
  }

  @Test
  private void testValidateValidParams() {
    AddGlossaryTermsAction action =
        new AddGlossaryTermsAction(mock(GlossaryTermServiceAsync.class));
    ActionParameters params = new ActionParameters(VALID_PARAMS);
    action.validate(params);
  }

  @Test
  private void testValidateInvalidParams() {
    AddGlossaryTermsAction action =
        new AddGlossaryTermsAction(mock(GlossaryTermServiceAsync.class));
    ActionParameters params = new ActionParameters(Collections.emptyMap());
    Assert.assertThrows(InvalidActionParamsException.class, () -> action.validate(params));
  }
}
