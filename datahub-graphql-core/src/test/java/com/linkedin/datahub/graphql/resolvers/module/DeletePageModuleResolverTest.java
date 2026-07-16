package com.linkedin.datahub.graphql.resolvers.module;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DeletePageModuleInput;
import com.linkedin.metadata.service.PageModuleService;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.testng.annotations.Test;

public class DeletePageModuleResolverTest {
  private static final String TEST_MODULE_URN = "urn:li:dataHubPageModule:test";

  @Test
  public void testGetSuccess() throws Exception {
    PageModuleService mockService = mock(PageModuleService.class);
    DeletePageModuleResolver resolver = new DeletePageModuleResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    DeletePageModuleInput input = new DeletePageModuleInput();
    input.setUrn(TEST_MODULE_URN);

    Urn urn = UrnUtils.getUrn(TEST_MODULE_URN);
    doNothing().when(mockService).deletePageModule(any(), eq(urn));

    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    Boolean result = resolver.get(mockEnv).join();
    assertTrue(result);
    verify(mockService, times(1)).deletePageModule(any(), eq(urn));
  }

  @Test
  public void testGetThrowsException() throws Exception {
    PageModuleService mockService = mock(PageModuleService.class);
    DeletePageModuleResolver resolver = new DeletePageModuleResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    DeletePageModuleInput input = new DeletePageModuleInput();
    input.setUrn(TEST_MODULE_URN);

    Urn urn = UrnUtils.getUrn(TEST_MODULE_URN);
    doThrow(new RuntimeException("fail")).when(mockService).deletePageModule(any(), eq(urn));

    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verify(mockService, times(1)).deletePageModule(any(), eq(urn));
  }
}
