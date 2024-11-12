package com.linkedin.metadata.test.action.tag;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.service.TagServiceAsync;
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

public class AddTagsActionTest {

  private static final List<Urn> TEST_TAGS =
      ImmutableList.of(UrnUtils.getUrn("urn:li:tag:test"), UrnUtils.getUrn("urn:li:tag:test2"));

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
      ImmutableMap.of("values", TEST_TAGS.stream().map(Urn::toString).collect(Collectors.toList()));

  @Test
  private void testApply() throws Exception {
    TagServiceAsync service = mock(TagServiceAsync.class);

    AddTagsAction action = new AddTagsAction(service);
    ActionParameters params = new ActionParameters(VALID_PARAMS);
    action.apply(mock(OperationContext.class), ALL_URNS, params);

    Mockito.verify(service, Mockito.atLeastOnce())
        .batchAddTags(
            any(OperationContext.class),
            Mockito.eq(TEST_TAGS),
            Mockito.eq(DASHBOARD_REFERENCES),
            Mockito.eq(METADATA_TESTS_SOURCE));

    Mockito.verify(service, Mockito.atLeastOnce())
        .batchAddTags(
            any(OperationContext.class),
            Mockito.eq(TEST_TAGS),
            Mockito.eq(DATASET_REFERENCES),
            Mockito.eq(METADATA_TESTS_SOURCE));

    Mockito.verifyNoMoreInteractions(service);
  }

  @Test
  private void testValidateValidParams() {
    AddTagsAction action = new AddTagsAction(mock(TagServiceAsync.class));
    ActionParameters params = new ActionParameters(VALID_PARAMS);
    action.validate(params);
  }

  @Test
  private void testValidateInvalidParams() {
    AddTagsAction action = new AddTagsAction(mock(TagServiceAsync.class));
    ActionParameters params = new ActionParameters(Collections.emptyMap());
    Assert.assertThrows(InvalidActionParamsException.class, () -> action.validate(params));
  }
}
