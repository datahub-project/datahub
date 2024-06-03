package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchAssignFormInput;
import com.linkedin.metadata.service.FormService;
import graphql.com.google.common.collect.ImmutableList;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class BatchRemoveFormResolverTest {

  private static final String TEST_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)";
  private static final String TEST_FORM_URN = "urn:li:form:1";

  private static final BatchAssignFormInput TEST_INPUT =
      new BatchAssignFormInput(TEST_FORM_URN, ImmutableList.of(TEST_DATASET_URN));

  @Test
  public void testGetSuccess() throws Exception {
    FormService mockFormService = initMockFormService(true);
    BatchRemoveFormResolver resolver = new BatchRemoveFormResolver(mockFormService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    boolean success = resolver.get(mockEnv).get();

    assertTrue(success);

    // Validate that we called unassign on the service
    Mockito.verify(mockFormService, Mockito.times(1))
        .batchUnassignFormForEntities(
            any(),
            Mockito.eq(ImmutableList.of(UrnUtils.getUrn(TEST_DATASET_URN))),
            Mockito.eq(UrnUtils.getUrn(TEST_FORM_URN)));
  }

  @Test
  public void testThrowsError() throws Exception {
    FormService mockFormService = initMockFormService(false);
    BatchRemoveFormResolver resolver = new BatchRemoveFormResolver(mockFormService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Validate that we called unassign on the service - but it throws an error
    Mockito.verify(mockFormService, Mockito.times(1))
        .batchUnassignFormForEntities(
            any(),
            Mockito.eq(ImmutableList.of(UrnUtils.getUrn(TEST_DATASET_URN))),
            Mockito.eq(UrnUtils.getUrn(TEST_FORM_URN)));
  }

  private FormService initMockFormService(final boolean shouldSucceed) throws Exception {
    FormService service = Mockito.mock(FormService.class);

    if (!shouldSucceed) {
      Mockito.doThrow(new RuntimeException())
          .when(service)
          .batchUnassignFormForEntities(any(), Mockito.any(), Mockito.any());
    }

    return service;
  }
}
