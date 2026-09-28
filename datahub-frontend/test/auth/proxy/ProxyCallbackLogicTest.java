package auth.proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import client.AuthServiceClient;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.aspect.CorpUserAspect;
import com.linkedin.metadata.aspect.CorpUserAspectArray;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import play.mvc.Http;
import play.mvc.Result;

class ProxyCallbackLogicTest {

  @Mock private SystemEntityClient systemEntityClient;
  @Mock private OperationContext systemOperationContext;
  @Mock private AuthServiceClient authClient;

  private ProxyCallbackLogic callbackLogic;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    Map<String, Object> configMap = new HashMap<>();
    configMap.put("auth.proxy.enabled", true);
    configMap.put("auth.proxy.userHeader", "X-Forwarded-User");
    configMap.put("auth.proxy.jitProvisioningEnabled", true);
    Config config = ConfigFactory.parseMap(configMap);

    callbackLogic =
        new ProxyCallbackLogic(systemEntityClient, systemOperationContext, authClient, config);
  }

  @Test
  public void testProvisionNewUser() throws Exception {
    CorpuserUrn urn = new CorpuserUrn("newuser");

    CorpUserSnapshot existingSnapshot = new CorpUserSnapshot();
    existingSnapshot.setUrn(urn);
    CorpUserAspectArray aspects = new CorpUserAspectArray();
    aspects.add(CorpUserAspect.create(new com.linkedin.identity.CorpUserInfo().setActive(false)));
    existingSnapshot.setAspects(aspects);

    Entity existingEntity = new Entity();
    existingEntity.setValue(Snapshot.create(existingSnapshot));

    when(systemEntityClient.get(eq(systemOperationContext), eq(urn))).thenReturn(existingEntity);
    when(authClient.generateSessionTokenForUser(eq("newuser"), any())).thenReturn("token");

    Http.Request request =
        new Http.RequestBuilder()
            .method("GET")
            .uri("/authenticate")
            .header("X-Forwarded-User", "newuser")
            .build();

    Result result = callbackLogic.handleProxyLogin(request, "/");

    assertEquals(303, result.status());
    verify(systemEntityClient).update(eq(systemOperationContext), any(Entity.class));
    verify(systemEntityClient)
        .ingestProposal(eq(systemOperationContext), any(MetadataChangeProposal.class));
  }

  @Test
  public void testExistingUserSkipsProvisioning() throws Exception {
    CorpuserUrn urn = new CorpuserUrn("existinguser");

    CorpUserSnapshot snapshot = new CorpUserSnapshot();
    snapshot.setUrn(urn);
    CorpUserAspectArray aspects = new CorpUserAspectArray();
    aspects.add(CorpUserAspect.create(new com.linkedin.identity.CorpUserInfo().setActive(true)));
    aspects.add(CorpUserAspect.create(new com.linkedin.identity.CorpUserEditableInfo()));
    snapshot.setAspects(aspects);

    Entity entity = new Entity();
    entity.setValue(Snapshot.create(snapshot));

    when(systemEntityClient.get(eq(systemOperationContext), eq(urn))).thenReturn(entity);
    when(authClient.generateSessionTokenForUser(eq("existinguser"), any())).thenReturn("token");

    Http.Request request =
        new Http.RequestBuilder()
            .method("GET")
            .uri("/authenticate")
            .header("X-Forwarded-User", "existinguser")
            .build();

    Result result = callbackLogic.handleProxyLogin(request, "/");

    assertEquals(303, result.status());
    verify(systemEntityClient, never()).update(any(), any(Entity.class));
    verify(systemEntityClient)
        .ingestProposal(eq(systemOperationContext), any(MetadataChangeProposal.class));
  }

  @Test
  public void testProvisionFailureReturnsInternalServerError() throws Exception {
    CorpuserUrn urn = new CorpuserUrn("failuser");

    when(systemEntityClient.get(eq(systemOperationContext), eq(urn)))
        .thenThrow(new RemoteInvocationException("connection failed"));

    Http.Request request =
        new Http.RequestBuilder()
            .method("GET")
            .uri("/authenticate")
            .header("X-Forwarded-User", "failuser")
            .build();

    Result result = callbackLogic.handleProxyLogin(request, "/");

    assertEquals(500, result.status());
    verify(authClient, never()).generateSessionTokenForUser(any(), any());
  }
}
