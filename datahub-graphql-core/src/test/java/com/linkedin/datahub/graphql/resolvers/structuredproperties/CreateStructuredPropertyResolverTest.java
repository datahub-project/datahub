package com.linkedin.datahub.graphql.resolvers.structuredproperties;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateStructuredPropertyInput;
import com.linkedin.datahub.graphql.generated.StructuredPropertyEntity;
import com.linkedin.datahub.graphql.generated.StructuredPropertySettingsInput;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class CreateStructuredPropertyResolverTest {
  private static final String TEST_STRUCTURED_PROPERTY_URN = "urn:li:structuredProperty:1";

  private static final CreateStructuredPropertyInput TEST_INPUT =
      new CreateStructuredPropertyInput(
          null,
          "io.acryl.test",
          "Display Name",
          "description",
          true,
          null,
          null,
          null,
          null,
          new ArrayList<>(),
          null);

  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockEntityClient = initMockEntityClient(true);
    CreateStructuredPropertyResolver resolver =
        new CreateStructuredPropertyResolver(mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    StructuredPropertyEntity prop = resolver.get(mockEnv).get();

    assertEquals(prop.getUrn(), TEST_STRUCTURED_PROPERTY_URN);

    // Validate that we called ingest
    Mockito.verify(mockEntityClient, Mockito.times(1))
        .batchIngestProposals(any(), Mockito.anyList(), Mockito.eq(false));
  }

  @Test
  public void testGetMismatchIdAndQualifiedName() throws Exception {
    EntityClient mockEntityClient = initMockEntityClient(true);
    CreateStructuredPropertyResolver resolver =
        new CreateStructuredPropertyResolver(mockEntityClient);

    CreateStructuredPropertyInput testInput =
        new CreateStructuredPropertyInput(
            "mismatched",
            "io.acryl.test",
            "Display Name",
            "description",
            true,
            null,
            null,
            null,
            null,
            new ArrayList<>(),
            null);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Validate ingest is not called
    Mockito.verify(mockEntityClient, Mockito.times(0))
        .batchIngestProposals(any(), Mockito.anyList(), Mockito.eq(false));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    EntityClient mockEntityClient = initMockEntityClient(true);
    CreateStructuredPropertyResolver resolver =
        new CreateStructuredPropertyResolver(mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockDenyContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Validate that we did NOT call ingest
    Mockito.verify(mockEntityClient, Mockito.times(0))
        .batchIngestProposals(any(), Mockito.anyList(), Mockito.eq(false));
  }

  @Test
  public void testGetFailure() throws Exception {
    EntityClient mockEntityClient = initMockEntityClient(false);
    CreateStructuredPropertyResolver resolver =
        new CreateStructuredPropertyResolver(mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Validate that ingest was called, but that caused a failure
    Mockito.verify(mockEntityClient, Mockito.times(1))
        .batchIngestProposals(any(), Mockito.anyList(), Mockito.eq(false));
  }

  @Test
  public void testGetInvalidSettingsInput() throws Exception {
    EntityClient mockEntityClient = initMockEntityClient(true);
    CreateStructuredPropertyResolver resolver =
        new CreateStructuredPropertyResolver(mockEntityClient);

    // if isHidden is true, other fields should not be true
    StructuredPropertySettingsInput settingsInput = new StructuredPropertySettingsInput();
    settingsInput.setIsHidden(true);
    settingsInput.setShowAsAssetBadge(true);

    CreateStructuredPropertyInput testInput =
        new CreateStructuredPropertyInput(
            null,
            "io.acryl.test",
            "Display Name",
            "description",
            true,
            null,
            null,
            null,
            null,
            new ArrayList<>(),
            settingsInput);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Validate ingest is not called
    Mockito.verify(mockEntityClient, Mockito.times(0))
        .batchIngestProposals(any(), Mockito.anyList(), Mockito.eq(false));
  }

  @Test
  public void testGetSuccessWithSettings() throws Exception {
    EntityClient mockEntityClient = initMockEntityClient(true);
    CreateStructuredPropertyResolver resolver =
        new CreateStructuredPropertyResolver(mockEntityClient);

    StructuredPropertySettingsInput settingsInput = new StructuredPropertySettingsInput();
    settingsInput.setShowAsAssetBadge(true);

    CreateStructuredPropertyInput testInput =
        new CreateStructuredPropertyInput(
            null,
            "io.acryl.test",
            "Display Name",
            "description",
            true,
            null,
            null,
            null,
            null,
            new ArrayList<>(),
            settingsInput);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    StructuredPropertyEntity prop = resolver.get(mockEnv).get();

    assertEquals(prop.getUrn(), TEST_STRUCTURED_PROPERTY_URN);

    // Validate that we called ingest
    Mockito.verify(mockEntityClient, Mockito.times(1))
        .batchIngestProposals(any(), Mockito.anyList(), Mockito.eq(false));
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
