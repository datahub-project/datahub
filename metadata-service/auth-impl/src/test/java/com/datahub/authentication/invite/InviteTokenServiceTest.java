package com.datahub.authentication.invite;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
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
  private static final String INVITE_TOKEN_URN_STRING = "urn:li:inviteToken:admin-invite-token";
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
  private InviteTokenService _inviteTokenService;
  private OperationContext opContext;

  @BeforeMethod
  public void setupTest() throws Exception {
    inviteTokenUrn = Urn.createFromString(INVITE_TOKEN_URN_STRING);
    roleUrn = Urn.createFromString(ROLE_URN_STRING);
    _entityClient = mock(EntityClient.class);
    _secretService = mock(SecretService.class);
    opContext = mock(OperationContext.class);
    _inviteTokenService = new InviteTokenService(_entityClient, _secretService);
  }

  @Test
  public void testGetInviteTokenUrnPasses() throws Exception {
    _inviteTokenService.getInviteTokenUrn(INVITE_TOKEN_STRING);
  }

  @Test
  public void testIsInviteTokenValidFalse() throws Exception {
    when(_entityClient.exists(any(OperationContext.class), eq(inviteTokenUrn))).thenReturn(false);

    assertFalse(_inviteTokenService.isInviteTokenValid(opContext, inviteTokenUrn));
  }

  @Test
  public void testIsInviteTokenValidTrue() throws Exception {
    when(_entityClient.exists(any(OperationContext.class), eq(inviteTokenUrn))).thenReturn(true);

    assertTrue(_inviteTokenService.isInviteTokenValid(opContext, inviteTokenUrn));
  }

  @Test
  public void testGetInviteTokenRoleNullEntity() throws Exception {
    when(_entityClient.getV2(
            any(OperationContext.class), eq(INVITE_TOKEN_ENTITY_NAME), eq(inviteTokenUrn), any()))
        .thenReturn(null);

    assertThrows(() -> _inviteTokenService.getInviteTokenRole(opContext, inviteTokenUrn));
  }

  @Test
  public void testGetInviteTokenRoleEmptyAspectMap() throws Exception {
    final EntityResponse entityResponse = new EntityResponse().setAspects(new EnvelopedAspectMap());

    when(_entityClient.getV2(
            any(OperationContext.class), eq(INVITE_TOKEN_ENTITY_NAME), eq(inviteTokenUrn), any()))
        .thenReturn(entityResponse);

    assertThrows(() -> _inviteTokenService.getInviteTokenRole(opContext, inviteTokenUrn));
  }

  @Test
  public void testGetInviteTokenRoleNoRole() throws Exception {
    final EntityResponse entityResponse = new EntityResponse();
    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    final InviteToken inviteTokenAspect = new InviteToken().setToken(ENCRYPTED_INVITE_TOKEN_STRING);
    aspectMap.put(
        INVITE_TOKEN_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(inviteTokenAspect.data())));
    entityResponse.setAspects(aspectMap);

    when(_entityClient.getV2(
            any(OperationContext.class), eq(INVITE_TOKEN_ENTITY_NAME), eq(inviteTokenUrn), any()))
        .thenReturn(entityResponse);

    Urn roleUrn = _inviteTokenService.getInviteTokenRole(opContext, inviteTokenUrn);
    assertNull(roleUrn);
  }

  @Test
  public void testGetInviteTokenRole() throws Exception {
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

    Urn roleUrn = _inviteTokenService.getInviteTokenRole(opContext, inviteTokenUrn);
    assertNotNull(roleUrn);
    assertEquals(roleUrn, this.roleUrn);
  }

  @Test
  public void getInviteTokenRoleUrnDoesNotExist() throws Exception {
    when(_entityClient.exists(any(OperationContext.class), eq(roleUrn))).thenReturn(false);

    assertThrows(() -> _inviteTokenService.getInviteToken(opContext, roleUrn.toString(), false));
  }

  @Test
  public void getInviteTokenRegenerate() throws Exception {
    final SearchResult searchResult = new SearchResult();
    searchResult.setEntities(new SearchEntityArray());
    when(opContext.getAuthentication()).thenReturn(SYSTEM_AUTHENTICATION);
    when(_entityClient.filter(
            eq(opContext), eq(INVITE_TOKEN_ENTITY_NAME), any(), any(), anyInt(), anyInt()))
        .thenReturn(searchResult);
    when(_secretService.generateUrlSafeToken(anyInt())).thenReturn(INVITE_TOKEN_STRING);
    when(_secretService.hashString(anyString())).thenReturn(HASHED_INVITE_TOKEN_STRING);
    when(_secretService.encrypt(anyString())).thenReturn(ENCRYPTED_INVITE_TOKEN_STRING);

    _inviteTokenService.getInviteToken(opContext, null, true);
    verify(_entityClient, times(1)).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void getInviteTokenEmptySearchResult() throws Exception {
    final SearchResult searchResult = new SearchResult();
    searchResult.setEntities(new SearchEntityArray());
    when(opContext.getAuthentication()).thenReturn(SYSTEM_AUTHENTICATION);
    when(_entityClient.filter(
            eq(opContext), eq(INVITE_TOKEN_ENTITY_NAME), any(), any(), anyInt(), anyInt()))
        .thenReturn(searchResult);
    when(_secretService.generateUrlSafeToken(anyInt())).thenReturn(INVITE_TOKEN_STRING);
    when(_secretService.hashString(anyString())).thenReturn(HASHED_INVITE_TOKEN_STRING);
    when(_secretService.encrypt(anyString())).thenReturn(ENCRYPTED_INVITE_TOKEN_STRING);

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

    when(_secretService.encrypt(anyString())).thenReturn(ENCRYPTED_INVITE_TOKEN_STRING);

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

    when(_secretService.decrypt(eq(ENCRYPTED_INVITE_TOKEN_STRING))).thenReturn(INVITE_TOKEN_STRING);

    assertEquals(_inviteTokenService.getInviteToken(opContext, null, false), INVITE_TOKEN_STRING);
  }
}
