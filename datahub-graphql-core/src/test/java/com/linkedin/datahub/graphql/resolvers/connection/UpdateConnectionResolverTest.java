package com.linkedin.datahub.graphql.resolvers.connection;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.connection.DataHubConnectionDetails;
import com.linkedin.connection.DataHubJsonConnection;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.DataHubConnection;
import com.linkedin.datahub.graphql.generated.DataHubConnectionDetailsType;
import com.linkedin.datahub.graphql.generated.DataHubJsonConnectionInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.UpdateDataHubConnectionInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.connection.ConnectionService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateConnectionResolverTest {

  private ConnectionService connectionService;
  private SecretService secretService;
  private FeatureFlags featureFlags;
  private UpdateConnectionResolver resolver;

  @BeforeMethod
  public void setUp() {
    connectionService = Mockito.mock(ConnectionService.class);
    secretService = Mockito.mock(SecretService.class);
    featureFlags = Mockito.mock(FeatureFlags.class);
    Mockito.when(secretService.encrypt("{}")).thenReturn("encrypted");
    Mockito.when(secretService.decrypt("encrypted")).thenReturn("{}");
    final DataHubConnectionDetails details =
        new DataHubConnectionDetails()
            .setName("Old Name")
            .setType(com.linkedin.connection.DataHubConnectionDetailsType.JSON)
            .setJson(new DataHubJsonConnection().setEncryptedBlob("encrypted"));
    when(connectionService.getConnectionDetails(
            any(), Mockito.eq(UrnUtils.getUrn("urn:li:dataHubConnection:test-id"))))
        .thenReturn(details);
    when(featureFlags.isSlackBotTokensObfuscationEnabled()).thenReturn(false);
    resolver = new UpdateConnectionResolver(connectionService, secretService, featureFlags);
  }

  @Test
  public void testGetAuthorized() throws Exception {
    // Mock inputs
    Urn connectionUrn = UrnUtils.getUrn("urn:li:dataHubConnection:test-id");
    Urn platformUrn = UrnUtils.getUrn("urn:li:dataPlatform:slack");

    final UpdateDataHubConnectionInput input = new UpdateDataHubConnectionInput();
    input.setUrn(connectionUrn.toString());
    input.setPlatformUrn(platformUrn.toString());
    input.setType(DataHubConnectionDetailsType.JSON);
    input.setName("New Name");
    input.setJson(new DataHubJsonConnectionInput("{}"));

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    final DataHubConnectionDetails newDetails =
        new DataHubConnectionDetails()
            .setName("New Name")
            .setType(com.linkedin.connection.DataHubConnectionDetailsType.JSON)
            .setJson(new DataHubJsonConnection().setEncryptedBlob("encrypted"));

    final DataPlatformInstance platformInstance =
        new DataPlatformInstance().setPlatform(platformUrn);

    when(connectionService.updateConnection(
            any(OperationContext.class),
            Mockito.eq(connectionUrn),
            Mockito.eq(platformUrn),
            Mockito.eq(com.linkedin.connection.DataHubConnectionDetailsType.JSON),
            Mockito.any(DataHubJsonConnection.class),
            Mockito.eq("New Name")))
        .thenReturn(connectionUrn);
    when(connectionService.getConnectionEntityResponse(
            any(OperationContext.class), Mockito.eq(connectionUrn)))
        .thenReturn(
            new EntityResponse()
                .setUrn(connectionUrn)
                .setEntityName(Constants.DATAHUB_CONNECTION_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setName(Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME)
                                .setValue(new Aspect(newDetails.data())),
                            Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setName(Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME)
                                .setValue(new Aspect(platformInstance.data()))))));

    DataHubConnection actual = resolver.get(mockEnv).get();

    Assert.assertEquals(actual.getType(), EntityType.DATAHUB_CONNECTION);
    Assert.assertEquals(actual.getUrn(), connectionUrn.toString());
    Assert.assertEquals(actual.getPlatform().getUrn(), platformUrn.toString());
    Assert.assertEquals(actual.getDetails().getName(), input.getName());
    Assert.assertEquals(actual.getDetails().getType(), input.getType());
    Assert.assertEquals(actual.getDetails().getJson().getBlob(), input.getJson().getBlob());
  }

  @Test
  public void testGetUnAuthorized() {
    // Mock inputs
    Urn connectionUrn = UrnUtils.getUrn("urn:li:dataHubConnection:test-id");

    final UpdateDataHubConnectionInput input = new UpdateDataHubConnectionInput();
    input.setUrn(connectionUrn.toString());
    input.setName("testing");
    input.setType(DataHubConnectionDetailsType.JSON);

    QueryContext mockContext = getMockDenyContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
