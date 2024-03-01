package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.group.GroupService;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchVerifyFormInput;
import com.linkedin.metadata.service.FormService;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class BatchVerifyFormResolverTest {

  private static final String TEST_DATASET_URN1 =
      "urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)";
  private static final String TEST_DATASET_URN2 =
      "urn:li:dataset:(urn:li:dataPlatform:hive,name2,PROD)";
  private static final List<String> ENTITY_URNS =
      ImmutableList.of(TEST_DATASET_URN1, TEST_DATASET_URN2);
  private static final String TEST_FORM_URN = "urn:li:form:1";

  @Test
  public void testGetSuccess() throws Exception {
    FormService mockFormService = initMockFormService(true, true);
    GroupService mockGroupService = initMockGroupService();
    BatchVerifyFormResolver resolver =
        new BatchVerifyFormResolver(mockFormService, mockGroupService);

    BatchVerifyFormInput input = generateInput();
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    boolean success = resolver.get(mockEnv).get();

    assertTrue(success);

    Mockito.verify(mockFormService, Mockito.times(1))
        .verifyFormForEntity(
            Mockito.eq(UrnUtils.getUrn(TEST_FORM_URN)),
            Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN1)),
            Mockito.any(Authentication.class));
    Mockito.verify(mockFormService, Mockito.times(1))
        .verifyFormForEntity(
            Mockito.eq(UrnUtils.getUrn(TEST_FORM_URN)),
            Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN2)),
            Mockito.any(Authentication.class));
  }

  @Test
  public void testNotAssigned() throws Exception {
    FormService mockFormService = initMockFormService(false, true);
    GroupService mockGroupService = initMockGroupService();
    BatchVerifyFormResolver resolver =
        new BatchVerifyFormResolver(mockFormService, mockGroupService);

    BatchVerifyFormInput input = generateInput();
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    Mockito.verify(mockFormService, Mockito.times(0))
        .verifyFormForEntity(Mockito.any(), Mockito.any(), Mockito.any(Authentication.class));
  }

  @Test
  public void testVerificationThrowsError() throws Exception {
    FormService mockFormService = initMockFormService(true, false);
    GroupService mockGroupService = initMockGroupService();
    BatchVerifyFormResolver resolver =
        new BatchVerifyFormResolver(mockFormService, mockGroupService);

    BatchVerifyFormInput input = generateInput();
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // gets called once but throws error
    Mockito.verify(mockFormService, Mockito.times(1))
        .verifyFormForEntity(Mockito.any(), Mockito.any(), Mockito.any(Authentication.class));
  }

  private FormService initMockFormService(
      final boolean isFormAssignedToUser, final boolean shouldVerify) throws Exception {
    FormService service = Mockito.mock(FormService.class);
    Mockito.when(
            service.isFormAssignedToUser(
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(Authentication.class)))
        .thenReturn(isFormAssignedToUser);

    if (shouldVerify) {
      Mockito.when(
              service.verifyFormForEntity(
                  Mockito.any(), Mockito.any(), Mockito.any(Authentication.class)))
          .thenReturn(true);
    } else {
      Mockito.when(
              service.verifyFormForEntity(
                  Mockito.any(), Mockito.any(), Mockito.any(Authentication.class)))
          .thenThrow(new RuntimeException());
    }

    return service;
  }

  private GroupService initMockGroupService() throws Exception {
    GroupService service = Mockito.mock(GroupService.class);
    Mockito.when(service.getGroupsForUser(Mockito.any(), Mockito.any(Authentication.class)))
        .thenReturn(new ArrayList<>());

    return service;
  }

  private BatchVerifyFormInput generateInput() {
    BatchVerifyFormInput input = new BatchVerifyFormInput();
    input.setAssetUrns(ENTITY_URNS);
    input.setFormUrn(TEST_FORM_URN);

    return input;
  }
}
