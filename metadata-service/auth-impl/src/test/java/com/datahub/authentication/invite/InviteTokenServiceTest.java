package com.datahub.authentication.invite;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.InviteToken;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class InviteTokenServiceTest {
  private static final String INVITE_TOKEN_ID = "admin-invite-token";
  private static final String INVITE_TOKEN_URN_STRING = "urn:li:inviteToken:" + INVITE_TOKEN_ID;
  private static final String ROLE_URN_STRING = "urn:li:dataHubRole:Admin";
  private static final String INVITE_TOKEN_STRING = "inviteToken";
  private static final String HASHED_INVITE_TOKEN_STRING = "hashedInviteToken";
  private static final String ENCRYPTED_INVITE_TOKEN_STRING = "encryptedInviteToken";
  private static final String DATAHUB_SYSTEM_CLIENT_ID = "__datahub_system";
  private static final Authentication SYSTEM_AUTHENTICATION =
      new Authentication(new Actor(ActorType.USER, DATAHUB_SYSTEM_CLIENT_ID), "");
  private Urn inviteTokenUrn;
  private Urn roleUrn;
  private EntityClient _entityClient;
  private SecretService _secretService;
  private OperationContext _systemOperationContext;
  private InviteTokenService _inviteTokenService;
  private OperationContext opContext;

  @BeforeMethod
  public void setupTest() throws Exception {
    inviteTokenUrn = Urn.createFromString(INVITE_TOKEN_URN_STRING);
    roleUrn = Urn.createFromString(ROLE_URN_STRING);
    _entityClient = mock(EntityClient.class);
    _secretService = mock(SecretService.class);
    opContext = mock(OperationContext.class);
    _systemOperationContext = mock(OperationContext.class);
    _inviteTokenService =
        new InviteTokenService(_entityClient, _secretService, _systemOperationContext);
  }

  @Test
  public void testGetInviteTokenUrnPasses() throws Exception {
    _inviteTokenService.getInviteTokenUrn(INVITE_TOKEN_STRING);
  }

  @Test
  public void testGetVerifiedInviteTokenNullEntity() throws Exception {
    when(_secretService.hashString(INVITE_TOKEN_STRING)).thenReturn(INVITE_TOKEN_ID);
    when(_entityClient.getV2(
            any(OperationContext.class), eq(INVITE_TOKEN_ENTITY_NAME), eq(inviteTokenUrn), any()))
        .thenReturn(null);

    assertNull(_inviteTokenService.getVerifiedInviteToken(opContext, INVITE_TOKEN_STRING));
  }

  @Test
  public void testGetVerifiedInviteTokenEmptyAspectMap() throws Exception {
    when(_secretService.hashString(INVITE_TOKEN_STRING)).thenReturn(INVITE_TOKEN_ID);
    when(_entityClient.getV2(
            any(OperationContext.class), eq(INVITE_TOKEN_ENTITY_NAME), eq(inviteTokenUrn), any()))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap()));

    assertNull(_inviteTokenService.getVerifiedInviteToken(opContext, INVITE_TOKEN_STRING));
  }

  @Test
  public void testGetVerifiedInviteToken() throws Exception {
    mockInviteTokenEntity(
        new InviteToken().setToken(ENCRYPTED_INVITE_TOKEN_STRING).setRole(roleUrn));
    when(_secretService.decrypt(eq(_systemOperationContext), eq(ENCRYPTED_INVITE_TOKEN_STRING)))
        .thenReturn(INVITE_TOKEN_STRING);

    final InviteToken inviteToken =
        _inviteTokenService.getVerifiedInviteToken(opContext, INVITE_TOKEN_STRING);
    assertNotNull(inviteToken);
    assertEquals(inviteToken.getRole(), roleUrn);
  }

  @Test
  public void testGetVerifiedInviteTokenStoredValueMismatch() throws Exception {
    mockInviteTokenEntity(
        new InviteToken().setToken(ENCRYPTED_INVITE_TOKEN_STRING).setRole(roleUrn));
    when(_secretService.decrypt(any(), eq(ENCRYPTED_INVITE_TOKEN_STRING)))
        .thenReturn("otherInviteToken");

    assertNull(_inviteTokenService.getVerifiedInviteToken(opContext, INVITE_TOKEN_STRING));
  }

  @Test
  public void testGetVerifiedInviteTokenUndecryptableStoredValue() throws Exception {
    mockInviteTokenEntity(
        new InviteToken().setToken(ENCRYPTED_INVITE_TOKEN_STRING).setRole(roleUrn));
    when(_secretService.decrypt(any(), eq(ENCRYPTED_INVITE_TOKEN_STRING)))
        .thenThrow(new RuntimeException("Failed to decrypt"));

    assertNull(_inviteTokenService.getVerifiedInviteToken(opContext, INVITE_TOKEN_STRING));
  }

  @Test
  public void getInviteTokenRoleUrnDoesNotExist() throws Exception {
    allowManagePolicies();
    when(_entityClient.exists(any(OperationContext.class), eq(roleUrn))).thenReturn(false);

    assertThrows(() -> _inviteTokenService.getInviteToken(opContext, roleUrn.toString(), false));
  }

  @Test
  public void getInviteTokenForRoleDeniedWithoutManagePolicies() throws Exception {
    denyManagePolicies();

    assertThrows(
        InviteTokenAuthorizationException.class,
        () -> _inviteTokenService.getInviteToken(opContext, ROLE_URN_STRING, true));
    verify(_entityClient, never()).filter(any(), any(), any(), any(), anyInt(), anyInt());
    verify(_entityClient, never()).ingestProposal(any(), any());
    verify(_secretService, never()).decrypt(any(), anyString());
    verify(_secretService, never()).generateUrlSafeToken(anyInt());
  }

  @Test
  public void getInviteTokenForRoleAllowedWithManagePolicies() throws Exception {
    allowManagePolicies();
    mockEmptySearchTokenCreation();

    assertEquals(
        _inviteTokenService.getInviteToken(opContext, ROLE_URN_STRING, true), INVITE_TOKEN_STRING);
    verify(_entityClient, times(1)).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void getInviteTokenExistingRoleDeniedBeforeDecrypt() throws Exception {
    denyManagePolicies();
    final SearchEntityArray entities = new SearchEntityArray();
    entities.add(new SearchEntity().setEntity(inviteTokenUrn));
    when(_entityClient.filter(
            eq(opContext), eq(INVITE_TOKEN_ENTITY_NAME), any(), any(), anyInt(), anyInt()))
        .thenReturn(new SearchResult().setEntities(entities));
    mockInviteTokenEntity(
        new InviteToken().setToken(ENCRYPTED_INVITE_TOKEN_STRING).setRole(roleUrn));
    when(_secretService.decrypt(any(), eq(ENCRYPTED_INVITE_TOKEN_STRING)))
        .thenReturn(INVITE_TOKEN_STRING);

    assertThrows(
        InviteTokenAuthorizationException.class,
        () -> _inviteTokenService.getInviteToken(opContext, ROLE_URN_STRING, false));
    verify(_entityClient, never()).filter(any(), any(), any(), any(), anyInt(), anyInt());
    verify(_secretService, never()).decrypt(any(), anyString());
  }

  @Test
  public void getInviteTokenWithoutRoleDoesNotRequireManagePolicies() throws Exception {
    mockEmptySearchTokenCreation();

    assertEquals(_inviteTokenService.getInviteToken(opContext, null, true), INVITE_TOKEN_STRING);
    assertEquals(_inviteTokenService.getInviteToken(opContext, "  ", false), INVITE_TOKEN_STRING);
    verify(opContext, never()).authorize(anyString(), nullable(EntitySpec.class), any());
  }

  private void allowManagePolicies() {
    when(opContext.authorize(eq("MANAGE_POLICIES"), nullable(EntitySpec.class), any()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
  }

  private void denyManagePolicies() {
    when(opContext.authorize(anyString(), nullable(EntitySpec.class), any()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.DENY, ""));
  }

  @Test
  public void getInviteTokenRegenerate() throws Exception {
    mockEmptySearchTokenCreation();

    _inviteTokenService.getInviteToken(opContext, null, true);
    verify(_entityClient, times(1)).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void getInviteTokenEmptySearchResult() throws Exception {
    mockEmptySearchTokenCreation();

    _inviteTokenService.getInviteToken(opContext, null, false);
    verify(_entityClient, times(1)).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void getInviteTokenNullEntity() throws Exception {
    final SearchResult searchResult = new SearchResult();
    final SearchEntityArray searchEntityArray = new SearchEntityArray();
    final SearchEntity searchEntity = new SearchEntity().setEntity(inviteTokenUrn);
    searchEntityArray.add(searchEntity);
    searchResult.setEntities(searchEntityArray);
    when(opContext.getAuthentication()).thenReturn(SYSTEM_AUTHENTICATION);
    when(_entityClient.filter(
            eq(opContext), eq(INVITE_TOKEN_ENTITY_NAME), any(), any(), anyInt(), anyInt()))
        .thenReturn(searchResult);
    when(_entityClient.getV2(
            any(OperationContext.class), eq(INVITE_TOKEN_ENTITY_NAME), eq(inviteTokenUrn), any()))
        .thenReturn(null);

    assertThrows(() -> _inviteTokenService.getInviteToken(opContext, null, false));
  }

  @Test
  public void getInviteTokenNoInviteTokenAspect() throws Exception {
    final SearchResult searchResult = new SearchResult();
    final SearchEntityArray searchEntityArray = new SearchEntityArray();
    final SearchEntity searchEntity = new SearchEntity().setEntity(inviteTokenUrn);
    searchEntityArray.add(searchEntity);
    searchResult.setEntities(searchEntityArray);
    when(_entityClient.filter(
            eq(opContext), eq(INVITE_TOKEN_ENTITY_NAME), any(), any(), anyInt(), anyInt()))
        .thenReturn(searchResult);

    final EntityResponse entityResponse = new EntityResponse().setAspects(new EnvelopedAspectMap());
    when(_entityClient.getV2(
            any(OperationContext.class), eq(INVITE_TOKEN_ENTITY_NAME), eq(inviteTokenUrn), any()))
        .thenReturn(entityResponse);

    when(_secretService.encrypt(any(), anyString())).thenReturn(ENCRYPTED_INVITE_TOKEN_STRING);

    assertThrows(() -> _inviteTokenService.getInviteToken(opContext, null, false));
  }

  @Test
  public void getInviteToken() throws Exception {
    final SearchResult searchResult = new SearchResult();
    final SearchEntityArray searchEntityArray = new SearchEntityArray();
    final SearchEntity searchEntity = new SearchEntity().setEntity(inviteTokenUrn);
    searchEntityArray.add(searchEntity);
    searchResult.setEntities(searchEntityArray);
    when(opContext.getAuthentication()).thenReturn(SYSTEM_AUTHENTICATION);
    when(_entityClient.filter(
            eq(opContext), eq(INVITE_TOKEN_ENTITY_NAME), any(), any(), anyInt(), anyInt()))
        .thenReturn(searchResult);

    final EntityResponse entityResponse = new EntityResponse();
    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    final InviteToken inviteTokenAspect =
        new InviteToken().setToken(ENCRYPTED_INVITE_TOKEN_STRING).setRole(roleUrn);
    aspectMap.put(
        INVITE_TOKEN_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(inviteTokenAspect.data())));
    entityResponse.setAspects(aspectMap);
    when(_entityClient.getV2(
            any(OperationContext.class), eq(INVITE_TOKEN_ENTITY_NAME), eq(inviteTokenUrn), any()))
        .thenReturn(entityResponse);

    when(_secretService.decrypt(any(), eq(ENCRYPTED_INVITE_TOKEN_STRING)))
        .thenReturn(INVITE_TOKEN_STRING);

    assertEquals(_inviteTokenService.getInviteToken(opContext, null, false), INVITE_TOKEN_STRING);
  }

  private void mockEmptySearchTokenCreation() throws Exception {
    final SearchResult searchResult = new SearchResult();
    searchResult.setEntities(new SearchEntityArray());
    when(opContext.getAuthentication()).thenReturn(SYSTEM_AUTHENTICATION);
    when(_entityClient.filter(
            eq(opContext), eq(INVITE_TOKEN_ENTITY_NAME), any(), any(), anyInt(), anyInt()))
        .thenReturn(searchResult);
    when(_secretService.generateUrlSafeToken(anyInt())).thenReturn(INVITE_TOKEN_STRING);
    when(_secretService.hashString(anyString())).thenReturn(HASHED_INVITE_TOKEN_STRING);
    when(_secretService.encrypt(any(), anyString())).thenReturn(ENCRYPTED_INVITE_TOKEN_STRING);
  }

  private void mockInviteTokenEntity(final InviteToken inviteTokenAspect) throws Exception {
    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        INVITE_TOKEN_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(inviteTokenAspect.data())));
    when(_secretService.hashString(INVITE_TOKEN_STRING)).thenReturn(INVITE_TOKEN_ID);
    when(_entityClient.getV2(
            any(OperationContext.class), eq(INVITE_TOKEN_ENTITY_NAME), eq(inviteTokenUrn), any()))
        .thenReturn(new EntityResponse().setAspects(aspectMap));
  }
}
