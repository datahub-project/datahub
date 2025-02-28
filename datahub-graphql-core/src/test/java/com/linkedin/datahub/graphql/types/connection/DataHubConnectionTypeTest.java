package com.linkedin.datahub.graphql.types.connection;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.connection.DataHubConnectionDetails;
import com.linkedin.connection.DataHubConnectionDetailsType;
import com.linkedin.connection.DataHubJsonConnection;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.DataHubConnection;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.execution.DataFetcherResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DataHubConnectionTypeTest {
  private final String TEST_CONNECTION_1_URN = "urn:li:dataHubConnection:slack";
  private final String ENCRYPTED_CONNECTION_BLOB = "encrypted_blob";
  private final String DECRYPTED_CONNECTION_BLOB =
      "{\"bot_token\": \"f3ufwhuqaghlwh4ug5o\", \"app_details\": {\"app_id\": \"A12345\", \"client_secret\": \"abcdefg1234567\", \"signing_secret\": \"hijklmn7654321\", \"verification_token\": \"opqrstu9876543\"}, \"app_config_tokens\": {\"access_token\": \"xoxa-1234567890-abcdefghijklmnop\", \"refresh_token\": \"xoxr-1234567890-qrstuvwxyz\"}}";
  private final String OBFUSCATED_CONNECTION_BLOB =
      "{\"bot_token\": \"f3****g5o\", \"app_details\": {\"app_id\": \"A12345\", \"client_secret\": \"ab****567\", \"signing_secret\": \"hi****321\", \"verification_token\": \"op****543\"}, \"app_config_tokens\": {\"access_token\": \"xo****nop\", \"refresh_token\": \"xo****xyz\"}}";
  private final DataHubConnectionDetails TEST_SLACK_CONNECTION_DETAILS =
      new DataHubConnectionDetails()
          .setType(DataHubConnectionDetailsType.JSON)
          .setName("slack")
          .setJson(new DataHubJsonConnection().setEncryptedBlob(ENCRYPTED_CONNECTION_BLOB));
  private final DataPlatformInstance TEST_CONNECTION_PLATFORM_INSTANCE =
      new DataPlatformInstance()
          .setInstance(UrnUtils.getUrn("urn:li:instance:slackWorkspace"))
          .setPlatform(UrnUtils.getUrn("urn:li:platform:slack"));

  @Test
  public void testBatchLoadWithObfuscation() throws Exception {

    final EntityClient client = Mockito.mock(EntityClient.class);
    final SecretService secretsService = Mockito.mock(SecretService.class);
    final FeatureFlags featureFlags = Mockito.mock(FeatureFlags.class);

    Mockito.when(secretsService.decrypt(ENCRYPTED_CONNECTION_BLOB))
        .thenReturn(DECRYPTED_CONNECTION_BLOB);
    Mockito.when(featureFlags.isSlackBotTokensObfuscationEnabled()).thenReturn(true);

    final Urn connectionUrn1 = Urn.createFromString(TEST_CONNECTION_1_URN);
    Mockito.when(
            client.batchGetV2(
                any(OperationContext.class),
                Mockito.eq(Constants.DATAHUB_CONNECTION_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(connectionUrn1))),
                Mockito.eq(DataHubConnectionType.ASPECTS_TO_FETCH)))
        .thenReturn(
            ImmutableMap.of(
                connectionUrn1,
                new EntityResponse()
                    .setEntityName(Constants.DATAHUB_CONNECTION_ENTITY_NAME)
                    .setUrn(connectionUrn1)
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(TEST_SLACK_CONNECTION_DETAILS.data())),
                                Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(
                                        new Aspect(TEST_CONNECTION_PLATFORM_INSTANCE.data())))))));

    final DataHubConnectionType type =
        new DataHubConnectionType(client, secretsService, featureFlags);

    QueryContext mockContext = getMockAllowContext();
    List<DataFetcherResult<DataHubConnection>> result =
        type.batchLoad(ImmutableList.of(TEST_CONNECTION_1_URN), mockContext);

    // Verify response
    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            any(OperationContext.class),
            Mockito.eq(Constants.DATAHUB_CONNECTION_ENTITY_NAME),
            Mockito.eq(ImmutableSet.of(connectionUrn1)),
            Mockito.eq(DataHubConnectionType.ASPECTS_TO_FETCH));

    assertEquals(result.size(), 1);

    DataHubConnection connection1 = result.get(0).getData();
    assertEquals(connection1.getUrn(), TEST_CONNECTION_1_URN);
    assertEquals(connection1.getType(), EntityType.DATAHUB_CONNECTION);
    final String resultBlob = connection1.getDetails().getJson().getBlob();
    final Map<String, Object> parsedResult =
        new ObjectMapper().readValue(resultBlob, HashMap.class);
    final Map<String, Object> expectedResult =
        new ObjectMapper().readValue(OBFUSCATED_CONNECTION_BLOB, HashMap.class);
    assertEquals(parsedResult, expectedResult);
  }
}
