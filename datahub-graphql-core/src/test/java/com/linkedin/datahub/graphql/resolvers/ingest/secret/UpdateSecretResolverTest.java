package com.linkedin.datahub.graphql.resolvers.ingest.secret;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.getMockDenyContext;
import static com.linkedin.metadata.Constants.SECRET_VALUE_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.when;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.UpdateSecretInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.secret.DataHubSecretValue;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.services.SecretService;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateSecretResolverTest {

  private static final Urn TEST_URN = UrnUtils.getUrn("urn:li:secret:secret-id");

  private static final UpdateSecretInput TEST_INPUT =
      new UpdateSecretInput(TEST_URN.toString(), "MY_SECRET", "mysecretvalue", "dummy");

  private DataFetchingEnvironment mockEnv;
  private EntityClient mockClient;
  private SecretService mockSecretService;
  private UpdateSecretResolver resolver;

  @BeforeMethod
  public void before() {
    mockClient = Mockito.mock(EntityClient.class);
    mockSecretService = Mockito.mock(SecretService.class);

    resolver = new UpdateSecretResolver(mockClient, mockSecretService);
  }

  private DataHubSecretValue createSecretAspect() {
    DataHubSecretValue secretAspect = new DataHubSecretValue();
    secretAspect.setValue("encryptedvalue.updated");
    secretAspect.setName(TEST_INPUT.getName() + ".updated");
    secretAspect.setDescription(TEST_INPUT.getDescription() + ".updated");
    secretAspect.setCreated(
        new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:test")).setTime(0L));
    return secretAspect;
  }

  @Test
  public void testGetSuccess() throws Exception {
    // with valid context
    QueryContext mockContext = getMockAllowContext();
    mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Mockito.when(mockClient.exists(any(), any())).thenReturn(true);
    Mockito.when(mockSecretService.encrypt(any())).thenReturn("encrypted_value");
    final EntityResponse entityResponse = new EntityResponse();
    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        SECRET_VALUE_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(createSecretAspect().data())));
    entityResponse.setAspects(aspectMap);

    when(mockClient.getV2(any(), any(), any(), any())).thenReturn(entityResponse);

    // Invoke the resolver
    resolver.get(mockEnv).join();
    Mockito.verify(mockClient, Mockito.times(1)).ingestProposal(any(), any(), anyBoolean());
  }

  @Test(
      description = "validate if nothing provided throws Exception",
      expectedExceptions = {AuthorizationException.class, CompletionException.class})
  public void testGetUnauthorized() throws Exception {
    // Execute resolver
    QueryContext mockContext = getMockDenyContext();
    mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).join();
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(any(), any(), anyBoolean());
  }
}
