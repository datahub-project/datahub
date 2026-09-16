package com.linkedin.datahub.graphql.resolvers.template;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DeletePageTemplateInput;
import com.linkedin.metadata.service.PageTemplateService;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.testng.annotations.Test;

public class DeletePageTemplateResolverTest {
  private static final String TEST_TEMPLATE_URN = "urn:li:dataHubPageTemplate:test";

  @Test
  public void testGetSuccess() throws Exception {
    PageTemplateService mockService = mock(PageTemplateService.class);
    DeletePageTemplateResolver resolver = new DeletePageTemplateResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    DeletePageTemplateInput input = new DeletePageTemplateInput();
    input.setUrn(TEST_TEMPLATE_URN);

    Urn urn = UrnUtils.getUrn(TEST_TEMPLATE_URN);
    doNothing().when(mockService).deletePageTemplate(any(), eq(urn));

    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    Boolean result = resolver.get(mockEnv).join();
    assertTrue(result);
    verify(mockService, times(1)).deletePageTemplate(any(), eq(urn));
  }

  @Test
  public void testGetThrowsException() throws Exception {
    PageTemplateService mockService = mock(PageTemplateService.class);
    DeletePageTemplateResolver resolver = new DeletePageTemplateResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    DeletePageTemplateInput input = new DeletePageTemplateInput();
    input.setUrn(TEST_TEMPLATE_URN);

    Urn urn = UrnUtils.getUrn(TEST_TEMPLATE_URN);
    doThrow(new RuntimeException("fail")).when(mockService).deletePageTemplate(any(), eq(urn));

    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verify(mockService, times(1)).deletePageTemplate(any(), eq(urn));
  }
}
