package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.group.GroupService;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.VerifyFormInput;
import com.linkedin.metadata.service.FormService;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class VerifyFormResolverTest {

  private static final String TEST_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)";
  private static final String TEST_FORM_URN = "urn:li:form:1";

  private static final VerifyFormInput TEST_INPUT =
      new VerifyFormInput(TEST_FORM_URN, TEST_DATASET_URN);

  @Test
  public void testGetSuccess() throws Exception {
    FormService mockFormService = initMockFormService(true, true);
    GroupService mockGroupService = initMockGroupService();
    VerifyFormResolver resolver = new VerifyFormResolver(mockFormService, mockGroupService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    boolean success = resolver.get(mockEnv).get();

    assertTrue(success);

    // Validate that we called verify on the service
    Mockito.verify(mockFormService, Mockito.times(1))
        .verifyFormForEntity(
            any(),
            Mockito.eq(UrnUtils.getUrn(TEST_FORM_URN)),
            Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    FormService mockFormService = initMockFormService(false, true);
    GroupService mockGroupService = initMockGroupService();
    VerifyFormResolver resolver = new VerifyFormResolver(mockFormService, mockGroupService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    // Validate that we do not call verify on the service
    Mockito.verify(mockFormService, Mockito.times(0))
        .verifyFormForEntity(any(), Mockito.any(), Mockito.any());
  }

  @Test
  public void testThrowErrorOnVerification() throws Exception {
    FormService mockFormService = initMockFormService(true, false);
    GroupService mockGroupService = initMockGroupService();
    VerifyFormResolver resolver = new VerifyFormResolver(mockFormService, mockGroupService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    // Validate that we do call verifyFormForEntity but an error is thrown
    Mockito.verify(mockFormService, Mockito.times(1))
        .verifyFormForEntity(any(), Mockito.any(), Mockito.any());
  }

  private FormService initMockFormService(
      final boolean isFormAssignedToUser, final boolean shouldVerify) throws Exception {
    FormService service = Mockito.mock(FormService.class);
    Mockito.when(
            service.isFormAssignedToUser(
                any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(isFormAssignedToUser);

    if (shouldVerify) {
      Mockito.when(service.verifyFormForEntity(any(), Mockito.any(), Mockito.any()))
          .thenReturn(true);
    } else {
      Mockito.when(service.verifyFormForEntity(any(), Mockito.any(), Mockito.any()))
          .thenThrow(new RuntimeException());
    }

    return service;
  }

  private GroupService initMockGroupService() throws Exception {
    GroupService service = Mockito.mock(GroupService.class);
    Mockito.when(service.getGroupsForUser(any(), Mockito.any())).thenReturn(new ArrayList<>());

    return service;
  }
}
