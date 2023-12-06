package com.linkedin.datahub.graphql.resolvers.ingest.secret;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.GetSecretValuesInput;
import com.linkedin.datahub.graphql.generated.SecretValue;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.secret.SecretService;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.secret.DataHubSecretValue;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashSet;
import java.util.List;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class GetSecretValuesResolverTest {

  private static final GetSecretValuesInput TEST_INPUT =
      new GetSecretValuesInput(ImmutableList.of(getTestSecretValue().getName()));

  @Test
  public void testGetSuccess() throws Exception {

    final String decryptedSecretValue = "mysecretvalue";

    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    SecretService mockSecretService = Mockito.mock(SecretService.class);
    Mockito.when(mockSecretService.decrypt(Mockito.eq(getTestSecretValue().getValue())))
        .thenReturn(decryptedSecretValue);

    DataHubSecretValue returnedValue = getTestSecretValue();

    Mockito.when(
            mockClient.batchGetV2(
                Mockito.eq(Constants.SECRETS_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(TEST_SECRET_URN))),
                Mockito.eq(ImmutableSet.of(Constants.SECRET_VALUE_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(
            ImmutableMap.of(
                TEST_SECRET_URN,
                new EntityResponse()
                    .setEntityName(Constants.SECRETS_ENTITY_NAME)
                    .setUrn(TEST_SECRET_URN)
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.SECRET_VALUE_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(returnedValue.data())))))));

    GetSecretValuesResolver resolver = new GetSecretValuesResolver(mockClient, mockSecretService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Data Assertions
    List<SecretValue> values = resolver.get(mockEnv).get();
    assertEquals(values.size(), 1);
    assertEquals(values.get(0).getName(), TEST_INPUT.getSecrets().get(0));
    assertEquals(values.get(0).getValue(), decryptedSecretValue);
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    SecretService mockSecretService = Mockito.mock(SecretService.class);
    GetSecretValuesResolver resolver = new GetSecretValuesResolver(mockClient, mockSecretService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0))
        .batchGetV2(
            Mockito.any(), Mockito.anySet(), Mockito.anySet(), Mockito.any(Authentication.class));
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(
            Mockito.any(), Mockito.anySet(), Mockito.anySet(), Mockito.any(Authentication.class));
    SecretService mockSecretService = Mockito.mock(SecretService.class);
    GetSecretValuesResolver resolver = new GetSecretValuesResolver(mockClient, mockSecretService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
  }
}
