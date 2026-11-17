package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AndFilterInput;
import com.linkedin.datahub.graphql.generated.CreateDynamicFormAssignmentInput;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.form.DynamicFormAssignment;
import com.linkedin.metadata.service.FormService;
import graphql.com.google.common.collect.ImmutableList;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class CreateDynamicFormAssignmentResolverTest {

  private static final String TEST_FORM_URN = "urn:li:form:1";

  @Test
  public void testGetSuccess() throws Exception {
    FormService mockFormService = initMockFormService(true);
    CreateDynamicFormAssignmentResolver resolver =
        new CreateDynamicFormAssignmentResolver(mockFormService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(createTestInput());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    boolean success = resolver.get(mockEnv).get();

    assertTrue(success);

    Mockito.verify(mockFormService, Mockito.times(1))
        .createDynamicFormAssignment(
            any(), any(DynamicFormAssignment.class), Mockito.eq(UrnUtils.getUrn(TEST_FORM_URN)));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    FormService mockFormService = initMockFormService(true);
    CreateDynamicFormAssignmentResolver resolver =
        new CreateDynamicFormAssignmentResolver(mockFormService);

    QueryContext mockContext = getMockDenyContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(createTestInput());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Validate that we did NOT write the dynamic assignment
    Mockito.verify(mockFormService, Mockito.times(0))
        .createDynamicFormAssignment(any(), any(DynamicFormAssignment.class), Mockito.any());
  }

  @Test
  public void testThrowsError() throws Exception {
    FormService mockFormService = initMockFormService(false);
    CreateDynamicFormAssignmentResolver resolver =
        new CreateDynamicFormAssignmentResolver(mockFormService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(createTestInput());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    Mockito.verify(mockFormService, Mockito.times(1))
        .createDynamicFormAssignment(
            any(), any(DynamicFormAssignment.class), Mockito.eq(UrnUtils.getUrn(TEST_FORM_URN)));
  }

  private static CreateDynamicFormAssignmentInput createTestInput() {
    FacetFilterInput criterion = new FacetFilterInput();
    criterion.setField("platform");
    criterion.setValues(ImmutableList.of("urn:li:dataPlatform:hive"));
    criterion.setCondition(FilterOperator.EQUAL);
    AndFilterInput andFilter = new AndFilterInput();
    andFilter.setAnd(ImmutableList.of(criterion));

    CreateDynamicFormAssignmentInput input = new CreateDynamicFormAssignmentInput();
    input.setFormUrn(TEST_FORM_URN);
    input.setOrFilters(ImmutableList.of(andFilter));
    return input;
  }

  private FormService initMockFormService(final boolean shouldSucceed) throws Exception {
    FormService service = Mockito.mock(FormService.class);

    if (!shouldSucceed) {
      Mockito.doThrow(new RuntimeException())
          .when(service)
          .createDynamicFormAssignment(any(), any(DynamicFormAssignment.class), Mockito.any());
    }

    return service;
  }
}
