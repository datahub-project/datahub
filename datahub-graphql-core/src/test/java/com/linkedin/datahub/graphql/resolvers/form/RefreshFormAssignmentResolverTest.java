package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.RefreshFormAssignmentInput;
import com.linkedin.metadata.service.FormService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class RefreshFormAssignmentResolverTest {
  private static final String TEST_FORM_URN = "urn:li:form:1";

  private static final RefreshFormAssignmentInput TEST_INPUT =
      new RefreshFormAssignmentInput(TEST_FORM_URN, true, false);

  @Test
  public void testGetSuccessWithDefaults() throws Exception {
    FormService mockFormService = initMockFormService(true);
    RefreshFormAssignmentResolver resolver = new RefreshFormAssignmentResolver(mockFormService);

    // Execute resolver
    final RefreshFormAssignmentInput input = new RefreshFormAssignmentInput();
    input.setUrn(TEST_FORM_URN);
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Boolean success = resolver.get(mockEnv).get();

    assertEquals(success, true);

    // Validate that we called refresh on the service with expected default
    Mockito.verify(mockFormService, Mockito.times(1))
        .refreshFormAssignment(
            any(OperationContext.class),
            Mockito.eq(UrnUtils.getUrn(TEST_FORM_URN)),
            Mockito.eq(false),
            Mockito.eq(true));
  }

  @Test
  public void testGetSuccessWithoutDefaults() throws Exception {
    FormService mockFormService = initMockFormService(true);
    RefreshFormAssignmentResolver resolver = new RefreshFormAssignmentResolver(mockFormService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Boolean success = resolver.get(mockEnv).get();

    assertEquals(success, true);

    // Validate that we called refresh on the service
    Mockito.verify(mockFormService, Mockito.times(1))
        .refreshFormAssignment(
            any(OperationContext.class),
            Mockito.eq(UrnUtils.getUrn(TEST_FORM_URN)),
            Mockito.eq(true),
            Mockito.eq(false));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    FormService mockFormService = initMockFormService(true);
    RefreshFormAssignmentResolver resolver = new RefreshFormAssignmentResolver(mockFormService);

    // Execute resolver
    QueryContext mockContext = getMockDenyContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Validate that we did NOT call create on the service
    Mockito.verify(mockFormService, Mockito.times(0))
        .refreshFormAssignment(
            any(OperationContext.class),
            Mockito.eq(UrnUtils.getUrn(TEST_FORM_URN)),
            Mockito.eq(true),
            Mockito.eq(false));
  }

  @Test
  public void testGetFailure() throws Exception {
    FormService mockFormService = initMockFormService(false);
    RefreshFormAssignmentResolver resolver = new RefreshFormAssignmentResolver(mockFormService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Validate that we called refresh on the service, even though it fails
    Mockito.verify(mockFormService, Mockito.times(1))
        .refreshFormAssignment(
            any(OperationContext.class),
            Mockito.eq(UrnUtils.getUrn(TEST_FORM_URN)),
            Mockito.eq(true),
            Mockito.eq(false));
  }

  private FormService initMockFormService(final boolean shouldSucceed) throws Exception {
    FormService service = Mockito.mock(FormService.class);

    if (shouldSucceed) {
      Mockito.when(
              service.refreshFormAssignment(any(), Mockito.any(), Mockito.any(), Mockito.any()))
          .thenReturn(new Thread());
    } else {
      Mockito.when(
              service.refreshFormAssignment(any(), Mockito.any(), Mockito.any(), Mockito.any()))
          .thenThrow(new RuntimeException());
    }

    return service;
  }
}
