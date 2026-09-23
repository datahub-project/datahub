package com.linkedin.metadata.authorization;

import static com.linkedin.metadata.Constants.CORP_USER_CREDENTIALS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATASET_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.identity.CorpUserCredentials;
import com.linkedin.identity.CorpUserInfo;
import io.datahubproject.metadata.context.OperationContext;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SensitiveAspectAuthUtilTest {

  private static final Urn USER_URN = UrnUtils.getUrn("urn:li:corpuser:victim");
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,sensitive,PROD)");

  private OperationContext opContext;
  private MockedStatic<AuthUtil> authUtilMock;

  @BeforeMethod
  public void setUp() {
    opContext = mock(OperationContext.class);
    when(opContext.isSystemAuth()).thenReturn(false);
    authUtilMock = Mockito.mockStatic(AuthUtil.class);
  }

  @AfterMethod
  public void tearDown() {
    authUtilMock.close();
  }

  private void grantManageUserCredentials(boolean granted) {
    authUtilMock
        .when(
            () ->
                AuthUtil.isAuthorized(
                    any(AuthorizationSession.class),
                    eq(PoliciesConfig.MANAGE_USER_CREDENTIALS_PRIVILEGE)))
        .thenReturn(granted);
  }

  @Test
  public void testSystemAuthAlwaysReads() {
    when(opContext.isSystemAuth()).thenReturn(true);
    assertTrue(
        SensitiveAspectAuthUtil.canReadAspect(
            opContext, USER_URN, CORP_USER_CREDENTIALS_ASPECT_NAME));
    authUtilMock.verifyNoInteractions();
  }

  @Test
  public void testNonSensitiveAspectNeedsNoExtraPrivilege() {
    assertTrue(
        SensitiveAspectAuthUtil.canReadAspect(opContext, USER_URN, CORP_USER_INFO_ASPECT_NAME));
    assertTrue(
        SensitiveAspectAuthUtil.canReadAspect(
            opContext, DATASET_URN, DATASET_PROPERTIES_ASPECT_NAME));
    authUtilMock.verifyNoInteractions();
  }

  @Test
  public void testCredentialsRequireManageUserCredentials() {
    grantManageUserCredentials(false);
    assertFalse(
        SensitiveAspectAuthUtil.canReadAspect(
            opContext, USER_URN, CORP_USER_CREDENTIALS_ASPECT_NAME));
    // OpenAPI path segments are matched case-insensitively, so the gate must be too.
    assertFalse(SensitiveAspectAuthUtil.canReadAspect(opContext, USER_URN, "corpusercredentials"));

    grantManageUserCredentials(true);
    assertTrue(
        SensitiveAspectAuthUtil.canReadAspect(
            opContext, USER_URN, CORP_USER_CREDENTIALS_ASPECT_NAME));
  }

  @Test
  public void testOmitFromAspectMap() {
    grantManageUserCredentials(false);
    Map<String, String> aspects = new LinkedHashMap<>();
    aspects.put(CORP_USER_INFO_ASPECT_NAME, "info");
    aspects.put(CORP_USER_CREDENTIALS_ASPECT_NAME, "secret");

    Map<String, String> filtered =
        SensitiveAspectAuthUtil.omitUnauthorizedAspects(opContext, USER_URN, aspects);
    assertEquals(filtered, Map.of(CORP_USER_INFO_ASPECT_NAME, "info"));

    // Entity types without sensitive aspects are returned untouched.
    Map<String, String> datasetAspects = Map.of(DATASET_PROPERTIES_ASPECT_NAME, "props");
    assertSame(
        SensitiveAspectAuthUtil.omitUnauthorizedAspects(opContext, DATASET_URN, datasetAspects),
        datasetAspects);
  }

  @Test
  public void testOmitFromEntityResponse() {
    grantManageUserCredentials(false);
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        CORP_USER_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(new CorpUserInfo().setActive(true).data())));
    aspectMap.put(
        CORP_USER_CREDENTIALS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(credentials().data())));
    EntityResponse response = new EntityResponse().setUrn(USER_URN).setAspects(aspectMap);

    Map<Urn, EntityResponse> filtered =
        SensitiveAspectAuthUtil.omitUnauthorizedAspects(
            opContext, new LinkedHashMap<>(Map.of(USER_URN, response)));

    assertEquals(filtered.get(USER_URN).getAspects().keySet(), Set.of(CORP_USER_INFO_ASPECT_NAME));
    assertNotSame(filtered.get(USER_URN), response);
    // The service's instance is left intact for callers that may legitimately read the aspect.
    assertEquals(
        response.getAspects().keySet(),
        Set.of(CORP_USER_INFO_ASPECT_NAME, CORP_USER_CREDENTIALS_ASPECT_NAME));
    assertTrue(
        ((DataMap) response.data().get("aspects"))
            .containsKey(CORP_USER_CREDENTIALS_ASPECT_NAME));
    assertEquals(filtered.get(USER_URN).getUrn(), USER_URN);
  }

  @Test
  public void testBlankNamesAndEmptyResponsesPassThrough() {
    assertTrue(SensitiveAspectAuthUtil.canReadAspect(opContext, USER_URN, null));
    assertTrue(SensitiveAspectAuthUtil.canReadAspect(opContext, USER_URN, " "));
    assertTrue(SensitiveAspectAuthUtil.canReadAspect(opContext, (String) null, "x"));
    assertNull(SensitiveAspectAuthUtil.omitUnauthorizedAspects(opContext, (EntityResponse) null));
    EntityResponse bare = new EntityResponse().setUrn(USER_URN);
    assertSame(SensitiveAspectAuthUtil.omitUnauthorizedAspects(opContext, bare), bare);
    Map<String, String> empty = Map.of();
    assertSame(SensitiveAspectAuthUtil.omitUnauthorizedAspects(opContext, USER_URN, empty), empty);
    authUtilMock.verifyNoInteractions();
  }

  private static CorpUserCredentials credentials() {
    return new CorpUserCredentials().setSalt("salt").setHashedPassword("hash");
  }
}
