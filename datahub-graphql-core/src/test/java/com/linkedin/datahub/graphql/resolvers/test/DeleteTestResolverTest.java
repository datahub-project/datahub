package com.linkedin.datahub.graphql.resolvers.test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
<<<<<<< HEAD
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.mxe.MetadataChangeProposal;
=======
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.client.EntityClient;
>>>>>>> oss_master
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DeleteTestResolverTest {

  private static final String TEST_URN = "urn:li:test:test-id";

  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    TestEngine mockEngine = Mockito.mock(TestEngine.class);
    DeleteTestResolver resolver = new DeleteTestResolver(mockClient, mockEngine);
    Urn urn = Urn.createFromString(TEST_URN);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockClient.exists(Mockito.eq(urn), Mockito.any(Authentication.class)))
        .thenReturn(true);

    assertTrue(resolver.get(mockEnv).join());

<<<<<<< HEAD
    MetadataChangeProposal expectedChangeProposal =
        AspectUtils.buildMetadataChangeProposal(
            urn, Constants.STATUS_ASPECT_NAME, new Status().setRemoved(true));

    Mockito.verify(mockClient, Mockito.times(1))
        .exists(Mockito.eq(urn), Mockito.any(Authentication.class));

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            Mockito.eq(expectedChangeProposal),
            Mockito.any(Authentication.class),
            Mockito.eq(true));
  }

  @Test
  public void testGetEntityDoesNotExist() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    TestEngine mockEngine = Mockito.mock(TestEngine.class);
    DeleteTestResolver resolver = new DeleteTestResolver(mockClient, mockEngine);
    Urn urn = Urn.createFromString(TEST_URN);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockClient.exists(Mockito.eq(urn), Mockito.any(Authentication.class)))
        .thenReturn(false);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    Mockito.verify(mockClient, Mockito.times(1))
        .exists(Mockito.eq(urn), Mockito.any(Authentication.class));

    Mockito.verifyNoMoreInteractions(mockClient);
=======
    Mockito.verify(mockClient, Mockito.times(1))
        .deleteEntity(
            Mockito.eq(Urn.createFromString(TEST_URN)), Mockito.any(Authentication.class));
>>>>>>> oss_master
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    TestEngine mockEngine = Mockito.mock(TestEngine.class);
    DeleteTestResolver resolver = new DeleteTestResolver(mockClient, mockEngine);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0))
        .deleteEntity(Mockito.any(), Mockito.any(Authentication.class));
  }
}
