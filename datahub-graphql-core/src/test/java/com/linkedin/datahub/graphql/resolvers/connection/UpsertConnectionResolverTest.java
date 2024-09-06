package com.linkedin.datahub.graphql.resolvers.connection;

import static com.linkedin.datahub.graphql.TestUtils.*;
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
import com.linkedin.datahub.graphql.generated.DataHubConnection;
import com.linkedin.datahub.graphql.generated.DataHubConnectionDetailsType;
import com.linkedin.datahub.graphql.generated.DataHubJsonConnectionInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.UpsertDataHubConnectionInput;
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

public class UpsertConnectionResolverTest {

  private ConnectionService connectionService;
  private SecretService secretService;
  private UpsertConnectionResolver resolver;

  @BeforeMethod
  public void setUp() {
    connectionService = Mockito.mock(ConnectionService.class);
    secretService = Mockito.mock(SecretService.class);
    Mockito.when(secretService.encrypt("{}")).thenReturn("encrypted");
    Mockito.when(secretService.decrypt("encrypted")).thenReturn("{}");
    resolver = new UpsertConnectionResolver(connectionService, secretService);
  }

  @Test
  public void testGetAuthorized() throws Exception {
    // Mock inputs
    Urn connectionUrn = UrnUtils.getUrn("urn:li:dataHubConnection:test-id");
    Urn platformUrn = UrnUtils.getUrn("urn:li:dataPlatform:slack");

    final UpsertDataHubConnectionInput input = new UpsertDataHubConnectionInput();
    input.setId(connectionUrn.getId());
    input.setPlatformUrn(platformUrn.toString());
    input.setType(DataHubConnectionDetailsType.JSON);
    input.setName("test-name");
    input.setJson(new DataHubJsonConnectionInput("{}"));

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    final DataHubConnectionDetails details =
        new DataHubConnectionDetails()
            .setType(com.linkedin.connection.DataHubConnectionDetailsType.JSON)
            .setJson(new DataHubJsonConnection().setEncryptedBlob("encrypted"));

    final DataPlatformInstance platformInstance =
        new DataPlatformInstance().setPlatform(platformUrn);

    when(connectionService.upsertConnection(
            any(OperationContext.class),
            Mockito.eq(input.getId()),
            Mockito.eq(platformUrn),
            Mockito.eq(details.getType()),
            Mockito.eq(details.getJson()),
            Mockito.any(String.class)))
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
                                .setValue(new Aspect(details.data())),
                            Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setName(Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME)
                                .setValue(new Aspect(platformInstance.data()))))));

    DataHubConnection actual = resolver.get(mockEnv).get();

    Assert.assertEquals(actual.getType(), EntityType.DATAHUB_CONNECTION);
    Assert.assertEquals(actual.getUrn(), connectionUrn.toString());
    Assert.assertEquals(actual.getPlatform().getUrn(), platformUrn.toString());
    Assert.assertEquals(actual.getDetails().getType(), input.getType());
    Assert.assertEquals(actual.getDetails().getJson().getBlob(), input.getJson().getBlob());
  }

  @Test
  public void testGetUnAuthorized() {
    // Mock inputs
    Urn connectionUrn = UrnUtils.getUrn("urn:li:dataHubConnection:test-id");

    final UpsertDataHubConnectionInput input = new UpsertDataHubConnectionInput();
    input.setId(connectionUrn.getId());
    input.setPlatformUrn(connectionUrn.toString());
    input.setType(DataHubConnectionDetailsType.JSON);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
