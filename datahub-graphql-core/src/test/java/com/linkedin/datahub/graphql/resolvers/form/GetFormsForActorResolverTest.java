package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.group.GroupService;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.GetFormsForActorResult;
import com.linkedin.datahub.graphql.resolvers.search.SearchUtils;
import com.linkedin.metadata.service.FormService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GetFormsForActorResolverTest {
  private static final Urn TEST_USER = UrnUtils.getUrn("urn:li:corpuser:test-1");
  private static final Urn TEST_FORM_1 = UrnUtils.getUrn("urn:li:form:1");
  private static final Urn TEST_FORM_2 = UrnUtils.getUrn("urn:li:form:2");
  private static final Urn TEST_FORM_3 = UrnUtils.getUrn("urn:li:form:3");

  @Test
  public void testGetSuccess() throws Exception {
    GroupService mockGroupService = mockGroupService(true);
    FormService mockFormService = mockFormService();

    QueryContext mockContext = getMockAllowContext(TEST_USER.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(null);

    GetFormsForActorResolver resolver =
        new GetFormsForActorResolver(mockGroupService, mockFormService);
    GetFormsForActorResult actualResult = resolver.get(mockEnv).get();
    assertEquals(actualResult.getFormsForActor().size(), 3);
    assertTrue(
        actualResult.getFormsForActor().stream()
            .anyMatch(f -> f.getForm().getUrn().equals(TEST_FORM_1.toString())));
    assertTrue(
        actualResult.getFormsForActor().stream()
            .anyMatch(f -> f.getForm().getUrn().equals(TEST_FORM_2.toString())));
    assertTrue(
        actualResult.getFormsForActor().stream()
            .anyMatch(f -> f.getForm().getUrn().equals(TEST_FORM_3.toString())));
  }

  @Test
  public void testGetFailure() throws Exception {
    GroupService mockGroupService = mockGroupService(false);
    FormService mockFormService = mockFormService();

    QueryContext mockContext = getMockAllowContext(TEST_USER.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(null);

    GetFormsForActorResolver resolver =
        new GetFormsForActorResolver(mockGroupService, mockFormService);
    Assert.assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private GroupService mockGroupService(final boolean shouldSucceed) throws Exception {
    GroupService groupService = Mockito.mock(GroupService.class);
    if (shouldSucceed) {
      Mockito.when(
              groupService.getGroupsForUser(any(OperationContext.class), Mockito.eq(TEST_USER)))
          .thenReturn(new ArrayList<>());
    } else {
      Mockito.when(
              groupService.getGroupsForUser(any(OperationContext.class), Mockito.eq(TEST_USER)))
          .thenThrow(RuntimeException.class);
    }

    return groupService;
  }

  private FormService mockFormService() throws Exception {
    FormService formService = Mockito.mock(FormService.class);

    // mock getting explicitly assigned forms to this user
    Mockito.when(
            formService.getFormsAssignedToActor(
                any(OperationContext.class), Mockito.eq(TEST_USER), Mockito.eq(new ArrayList<>())))
        .thenReturn(ImmutableList.of(TEST_FORM_1));

    // mock getting implicitly assigned forms to this user
    Mockito.when(formService.getOwnershipForms(any(OperationContext.class)))
        .thenReturn(ImmutableList.of());
    Mockito.when(
            formService.getFormsAssignedByOwnership(
                any(OperationContext.class),
                Mockito.eq(SearchUtils.getEntityNames(null)),
                Mockito.eq(TEST_USER),
                Mockito.eq(ImmutableList.of()),
                Mockito.eq(ImmutableList.of())))
        .thenReturn(ImmutableList.of(TEST_FORM_2, TEST_FORM_3));

    return formService;
  }
}
