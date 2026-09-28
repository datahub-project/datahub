package com.linkedin.metadata.kafka.hydrator;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class EntityHydratorTest {
  private static final String USER_URN = "urn:li:corpuser:jdoe";

  private SystemEntityClient entityClient;
  private OperationContext opContext;
  private EntityHydrator hydrator;

  @BeforeMethod
  public void setUp() {
    entityClient = Mockito.mock(SystemEntityClient.class);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    hydrator = new EntityHydrator(TestOperationContexts.defaultEntityRegistry(), entityClient);
  }

  @DataProvider(name = "syntheticActors")
  public Object[][] syntheticActors() {
    return new Object[][] {{SYSTEM_ACTOR}, {ANONYMOUS_ACTOR}, {UNKNOWN_ACTOR}};
  }

  @Test(dataProvider = "syntheticActors")
  public void testSkipsHydrationForSyntheticActors(String actorUrn) throws Exception {
    Optional<ObjectNode> result = hydrator.getHydratedEntity(opContext, actorUrn);

    assertFalse(result.isPresent());
    verify(entityClient, never()).getV2(any(), any(), any());
  }

  @Test
  public void testMissingEntityReturnsEmpty() throws Exception {
    when(entityClient.getV2(any(OperationContext.class), eq(UrnUtils.getUrn(USER_URN)), any()))
        .thenReturn(null);

    Optional<ObjectNode> result = hydrator.getHydratedEntity(opContext, USER_URN);

    assertFalse(result.isPresent());
    verify(entityClient).getV2(any(OperationContext.class), eq(UrnUtils.getUrn(USER_URN)), any());
  }

  @Test
  public void testGmsExceptionReturnsEmpty() throws Exception {
    when(entityClient.getV2(any(OperationContext.class), eq(UrnUtils.getUrn(USER_URN)), any()))
        .thenThrow(new RemoteInvocationException("gms unavailable"));

    Optional<ObjectNode> result = hydrator.getHydratedEntity(opContext, USER_URN);

    assertFalse(result.isPresent());
  }

  @Test
  public void testHydratesCorpUser() throws Exception {
    Urn userUrn = UrnUtils.getUrn(USER_URN);
    when(entityClient.getV2(any(OperationContext.class), eq(userUrn), any()))
        .thenReturn(corpUserResponse(userUrn, "Jane Doe", "jdoe"));

    Optional<ObjectNode> result = hydrator.getHydratedEntity(opContext, USER_URN);

    assertTrue(result.isPresent());
    assertEquals(result.get().get("name").asText(), "Jane Doe");
    assertEquals(result.get().get("username").asText(), "jdoe");
  }

  private static EntityResponse corpUserResponse(Urn userUrn, String displayName, String username) {
    CorpUserInfo corpUserInfo = new CorpUserInfo();
    corpUserInfo.setActive(true);
    corpUserInfo.setDisplayName(displayName);

    CorpUserKey corpUserKey = new CorpUserKey();
    corpUserKey.setUsername(username);

    Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        CORP_USER_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(corpUserInfo.data())));
    aspects.put(
        CORP_USER_KEY_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(corpUserKey.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(userUrn);
    entityResponse.setEntityName(CORP_USER_ENTITY_NAME);
    entityResponse.setAspects(new EnvelopedAspectMap(aspects));
    return entityResponse;
  }
}
