package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Form;
import com.linkedin.datahub.graphql.generated.UpdateFormInput;
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

public class UpdateFormResolverTest {
  private static final String TEST_FORM_URN = "urn:li:form:1";

  private static final UpdateFormInput TEST_INPUT =
      new UpdateFormInput(TEST_FORM_URN, "new name", null, null, null, null, null);

  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockEntityClient = initMockEntityClient(true);
    UpdateFormResolver resolver = new UpdateFormResolver(mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Form form = resolver.get(mockEnv).get();

    assertEquals(form.getUrn(), TEST_FORM_URN);

    // Validate that we called ingest
    Mockito.verify(mockEntityClient, Mockito.times(1))
        .ingestProposal(any(), any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    EntityClient mockEntityClient = initMockEntityClient(true);
    UpdateFormResolver resolver = new UpdateFormResolver(mockEntityClient);

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
    UpdateFormResolver resolver = new UpdateFormResolver(mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Validate that ingest was called, but that caused a failure
    Mockito.verify(mockEntityClient, Mockito.times(1))
        .ingestProposal(any(), any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  private EntityClient initMockEntityClient(boolean shouldSucceed) throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);
    EntityResponse response = new EntityResponse();
    response.setEntityName(Constants.FORM_ENTITY_NAME);
    response.setUrn(UrnUtils.getUrn(TEST_FORM_URN));
    response.setAspects(new EnvelopedAspectMap());
    if (shouldSucceed) {
      Mockito.when(
              client.getV2(any(), Mockito.eq(Constants.FORM_ENTITY_NAME), any(), Mockito.eq(null)))
          .thenReturn(response);
    } else {
      Mockito.when(
              client.getV2(any(), Mockito.eq(Constants.FORM_ENTITY_NAME), any(), Mockito.eq(null)))
          .thenThrow(new RemoteInvocationException());
    }

    Mockito.when(client.exists(any(), Mockito.eq(UrnUtils.getUrn(TEST_FORM_URN)))).thenReturn(true);

    return client;
  }
}
