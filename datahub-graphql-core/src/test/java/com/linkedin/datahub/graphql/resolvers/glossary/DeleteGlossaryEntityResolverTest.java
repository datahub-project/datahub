package com.linkedin.datahub.graphql.resolvers.glossary;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockEntityService;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.VersionProperties;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.versioning.EntityVersioningService;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DeleteGlossaryEntityResolverTest {

  private static final String TEST_TERM_URN =
      "urn:li:glossaryTerm:12372c2ec7754c308993202dc44f548b";

  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityClient.ReferencesCleanup references = Mockito.mock(EntityClient.ReferencesCleanup.class);
    Mockito.when(mockClient.deleteEntityThenReferences(any(), any())).thenReturn(references);
    EntityService<?> mockService = getMockEntityService();
    EntityVersioningService mockVersioningService = Mockito.mock(EntityVersioningService.class);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_TERM_URN)), eq(true)))
        .thenReturn(true);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_TERM_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    DeleteGlossaryEntityResolver resolver =
        new DeleteGlossaryEntityResolver(mockClient, mockService, mockVersioningService);
    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockClient, Mockito.times(1))
        .deleteEntityThenReferences(any(), Mockito.eq(Urn.createFromString(TEST_TERM_URN)));
    Mockito.verifyNoInteractions(mockVersioningService);
  }

  @Test
  public void testDeletingAVersionUnlinksItBeforeDeleting() throws Exception {
    final Urn termUrn = Urn.createFromString(TEST_TERM_URN);
    final Urn versionSetUrn = UrnUtils.getUrn("urn:li:versionSet:(my_set,glossaryTerm)");
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(mockClient.deleteEntityThenReferences(any(), any()))
        .thenReturn(Mockito.mock(EntityClient.ReferencesCleanup.class));
    EntityService<?> mockService = getMockEntityService();
    Mockito.when(mockService.exists(any(), eq(termUrn), eq(true))).thenReturn(true);
    Mockito.when(mockService.getAspect(any(), eq(termUrn), eq("versionProperties"), eq(0L)))
        .thenReturn(new VersionProperties().setVersionSet(versionSetUrn));
    EntityVersioningService mockVersioningService = Mockito.mock(EntityVersioningService.class);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_TERM_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    DeleteGlossaryEntityResolver resolver =
        new DeleteGlossaryEntityResolver(mockClient, mockService, mockVersioningService);
    assertTrue(resolver.get(mockEnv).get());

    // Unlinking first hands "latest" to the remaining versions; deleting alone would leave the
    // set pointing at the deleted term and its other versions hidden.
    InOrder order = Mockito.inOrder(mockVersioningService, mockClient);
    order.verify(mockVersioningService).unlinkVersion(any(), eq(versionSetUrn), eq(termUrn));
    order.verify(mockClient).deleteEntityThenReferences(any(), eq(termUrn));
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .deleteEntityThenReferences(any(), Mockito.any());

    EntityService<?> mockService = getMockEntityService();
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_TERM_URN)), eq(true)))
        .thenReturn(true);
    EntityVersioningService mockVersioningService = Mockito.mock(EntityVersioningService.class);

    DeleteGlossaryEntityResolver resolver =
        new DeleteGlossaryEntityResolver(mockClient, mockService, mockVersioningService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_TERM_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
