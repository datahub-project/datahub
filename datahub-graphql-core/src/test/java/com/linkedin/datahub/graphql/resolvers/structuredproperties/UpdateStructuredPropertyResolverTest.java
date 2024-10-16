package com.linkedin.datahub.graphql.resolvers.structuredproperties;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.StructuredPropertyEntity;
import com.linkedin.datahub.graphql.generated.UpdateStructuredPropertyInput;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UpdateStructuredPropertyResolverTest {
  private static final String TEST_STRUCTURED_PROPERTY_URN = "urn:li:structuredProperty:1";

  private static final UpdateStructuredPropertyInput TEST_INPUT =
      new UpdateStructuredPropertyInput(
          TEST_STRUCTURED_PROPERTY_URN,
          "New Display Name",
          "new description",
          true,
          null,
          null,
          null,
          null);

  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockEntityClient = initMockEntityClient(true);
    UpdateStructuredPropertyResolver resolver =
        new UpdateStructuredPropertyResolver(mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    StructuredPropertyEntity prop = resolver.get(mockEnv).get();

    assertEquals(prop.getUrn(), TEST_STRUCTURED_PROPERTY_URN);

    // Validate that we called ingest
    Mockito.verify(mockEntityClient, Mockito.times(1))
        .ingestProposal(any(), any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    EntityClient mockEntityClient = initMockEntityClient(true);
    UpdateStructuredPropertyResolver resolver =
        new UpdateStructuredPropertyResolver(mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockDenyContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Validate that we did NOT call ingest
    Mockito.verify(mockEntityClient, Mockito.times(0))
        .ingestProposal(any(), any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  public void testGetFailure() throws Exception {
    EntityClient mockEntityClient = initMockEntityClient(false);
    UpdateStructuredPropertyResolver resolver =
        new UpdateStructuredPropertyResolver(mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Validate that ingest was not called since there was a get failure before ingesting
    Mockito.verify(mockEntityClient, Mockito.times(0))
        .ingestProposal(any(), any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  private EntityClient initMockEntityClient(boolean shouldSucceed) throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);
    EntityResponse response = new EntityResponse();
    response.setEntityName(Constants.STRUCTURED_PROPERTY_ENTITY_NAME);
    response.setUrn(UrnUtils.getUrn(TEST_STRUCTURED_PROPERTY_URN));
    response.setAspects(new EnvelopedAspectMap());
    if (shouldSucceed) {
      Mockito.when(
              client.getV2(
                  any(),
                  Mockito.eq(Constants.STRUCTURED_PROPERTY_ENTITY_NAME),
                  any(),
                  Mockito.eq(null)))
          .thenReturn(response);
    } else {
      Mockito.when(
              client.getV2(
                  any(),
                  Mockito.eq(Constants.STRUCTURED_PROPERTY_ENTITY_NAME),
                  any(),
                  Mockito.eq(null)))
          .thenThrow(new RemoteInvocationException());
    }

    return client;
  }
}
