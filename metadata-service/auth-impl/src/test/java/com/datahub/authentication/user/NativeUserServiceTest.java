package com.datahub.authentication.user;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.CorpUserCredentials;
import com.linkedin.identity.InviteToken;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.secret.SecretService;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class NativeUserServiceTest {
  private static final String DATAHUB_SYSTEM_CLIENT_ID = "__datahub_system";

  private static final String USER_URN_STRING = "urn:li:corpuser:test";
  private static final String FULL_NAME = "MOCK NAME";
  private static final String EMAIL = "mock@email.com";
  private static final String TITLE = "Data Scientist";
  private static final String PASSWORD = "password";
  private static final String INVITE_TOKEN = "inviteToken";
  private static final String ENCRYPTED_INVITE_TOKEN = "encryptedInviteToken";
  private static final String RESET_TOKEN = "inviteToken";
  private static final String ENCRYPTED_RESET_TOKEN = "encryptedInviteToken";
  private static final String ENCRYPTED_SALT = "encryptedSalt";
  private static final Urn USER_URN = new CorpuserUrn(EMAIL);
  private static final long ONE_DAY_MILLIS = TimeUnit.DAYS.toMillis(1);
  private static final Authentication SYSTEM_AUTHENTICATION =
      new Authentication(new Actor(ActorType.USER, DATAHUB_SYSTEM_CLIENT_ID), "");

  private EntityService _entityService;
  private EntityClient _entityClient;
  private SecretService _secretService;
  private NativeUserService _nativeUserService;

  @BeforeMethod
  public void setupTest() throws Exception {
    _entityService = mock(EntityService.class);
    _entityClient = mock(EntityClient.class);
    _secretService = mock(SecretService.class);

    _nativeUserService = new NativeUserService(_entityService, _entityClient, _secretService);
  }

  @Test
  public void testConstructor() throws Exception {
    assertThrows(() -> new NativeUserService(null, _entityClient, _secretService));
    assertThrows(() -> new NativeUserService(_entityService, null, _secretService));
    assertThrows(() -> new NativeUserService(_entityService, _entityClient, null));

    // Succeeds!
    new NativeUserService(_entityService, _entityClient, _secretService);
  }

  @Test
  public void testCreateNativeUserNullArguments() {
    assertThrows(() -> _nativeUserService.createNativeUser(null, FULL_NAME, EMAIL, TITLE, PASSWORD, INVITE_TOKEN,
        SYSTEM_AUTHENTICATION));
    assertThrows(() -> _nativeUserService.createNativeUser(USER_URN_STRING, null, EMAIL, TITLE, PASSWORD, INVITE_TOKEN,
        SYSTEM_AUTHENTICATION));
    assertThrows(
        () -> _nativeUserService.createNativeUser(USER_URN_STRING, FULL_NAME, null, TITLE, PASSWORD, INVITE_TOKEN,
            SYSTEM_AUTHENTICATION));
    assertThrows(
        () -> _nativeUserService.createNativeUser(USER_URN_STRING, FULL_NAME, EMAIL, null, PASSWORD, INVITE_TOKEN,
            SYSTEM_AUTHENTICATION));
    assertThrows(() -> _nativeUserService.createNativeUser(USER_URN_STRING, FULL_NAME, EMAIL, TITLE, null, INVITE_TOKEN,
        SYSTEM_AUTHENTICATION));
    assertThrows(() -> _nativeUserService.createNativeUser(USER_URN_STRING, FULL_NAME, EMAIL, TITLE, PASSWORD, null,
        SYSTEM_AUTHENTICATION));
  }

  @Test(expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = "Invalid sign-up token. Please ask your administrator to send you an updated link!")
  public void testCreateNativeUserInviteTokenDoesNotExist() throws Exception {
    // Nonexistent invite token
    when(_entityService.getLatestAspect(any(), eq(INVITE_TOKEN_ASPECT_NAME))).thenReturn(null);

    _nativeUserService.createNativeUser(USER_URN_STRING, FULL_NAME, EMAIL, TITLE, PASSWORD, INVITE_TOKEN,
        SYSTEM_AUTHENTICATION);
  }

  @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "This user already exists! Cannot create a new user.")
  public void testCreateNativeUserUserAlreadyExists() throws Exception {
    InviteToken mockInviteTokenAspect = mock(InviteToken.class);
    when(_entityService.getLatestAspect(any(), eq(INVITE_TOKEN_ASPECT_NAME))).thenReturn(mockInviteTokenAspect);
    when(mockInviteTokenAspect.hasToken()).thenReturn(true);
    when(mockInviteTokenAspect.getToken()).thenReturn(ENCRYPTED_INVITE_TOKEN);
    when(_secretService.decrypt(eq(ENCRYPTED_INVITE_TOKEN))).thenReturn(INVITE_TOKEN);

    // The user already exists
    when(_entityService.exists(any())).thenReturn(true);

    _nativeUserService.createNativeUser(USER_URN_STRING, FULL_NAME, EMAIL, TITLE, PASSWORD, INVITE_TOKEN,
        SYSTEM_AUTHENTICATION);
  }

  @Test
  public void testCreateNativeUserPasses() throws Exception {
    InviteToken mockInviteTokenAspect = mock(InviteToken.class);
    when(_entityService.getLatestAspect(any(), eq(INVITE_TOKEN_ASPECT_NAME))).thenReturn(mockInviteTokenAspect);
    when(mockInviteTokenAspect.hasToken()).thenReturn(true);
    when(mockInviteTokenAspect.getToken()).thenReturn(ENCRYPTED_INVITE_TOKEN);
    when(_entityService.exists(any())).thenReturn(false);
    when(_secretService.decrypt(eq(ENCRYPTED_INVITE_TOKEN))).thenReturn(INVITE_TOKEN);
    when(_secretService.encrypt(any())).thenReturn(ENCRYPTED_SALT);

    _nativeUserService.createNativeUser(USER_URN_STRING, FULL_NAME, EMAIL, TITLE, PASSWORD, INVITE_TOKEN,
        SYSTEM_AUTHENTICATION);
  }

  @Test
  public void testUpdateCorpUserInfoPasses() throws Exception {
    _nativeUserService.updateCorpUserInfo(USER_URN, FULL_NAME, EMAIL, TITLE, SYSTEM_AUTHENTICATION);
    verify(_entityClient).ingestProposal(any(), any());
  }

  @Test
  public void testUpdateCorpUserStatusPasses() throws Exception {
    _nativeUserService.updateCorpUserStatus(USER_URN, SYSTEM_AUTHENTICATION);
    verify(_entityClient).ingestProposal(any(), any());
  }

  @Test
  public void testUpdateCorpUserCredentialsPasses() throws Exception {
    when(_secretService.encrypt(any())).thenReturn(ENCRYPTED_SALT);

    _nativeUserService.updateCorpUserCredentials(USER_URN, PASSWORD, SYSTEM_AUTHENTICATION);
    verify(_entityClient).ingestProposal(any(), any());
  }

  @Test
  public void testGenerateNativeUserInviteTokenPasses() throws Exception {
    when(_secretService.encrypt(any())).thenReturn(ENCRYPTED_INVITE_TOKEN);

    _nativeUserService.generateNativeUserInviteToken(SYSTEM_AUTHENTICATION);
    verify(_entityClient).ingestProposal(any(), any());
  }

  @Test
  public void testGetNativeUserInviteTokenInviteTokenDoesNotExistPasses() throws Exception {
    // Nonexistent invite token
    when(_entityService.getLatestAspect(any(), eq(INVITE_TOKEN_ASPECT_NAME))).thenReturn(null);
    when(_secretService.encrypt(any())).thenReturn(ENCRYPTED_INVITE_TOKEN);

    _nativeUserService.getNativeUserInviteToken(SYSTEM_AUTHENTICATION);
    verify(_entityClient).ingestProposal(any(), any());
  }

  @Test
  public void testGetNativeUserInviteTokenPasses() throws Exception {
    InviteToken mockInviteTokenAspect = mock(InviteToken.class);
    when(_entityService.getLatestAspect(any(), eq(INVITE_TOKEN_ASPECT_NAME))).thenReturn(mockInviteTokenAspect);
    when(_entityService.exists(any())).thenReturn(false);
    when(mockInviteTokenAspect.hasToken()).thenReturn(true);
    when(mockInviteTokenAspect.getToken()).thenReturn(ENCRYPTED_INVITE_TOKEN);
    when(_secretService.decrypt(eq(ENCRYPTED_INVITE_TOKEN))).thenReturn(INVITE_TOKEN);

    assertEquals(_nativeUserService.getNativeUserInviteToken(SYSTEM_AUTHENTICATION), INVITE_TOKEN);
  }

  @Test
  public void testGenerateNativeUserResetTokenNullArguments() {
    assertThrows(() -> _nativeUserService.generateNativeUserPasswordResetToken(null, SYSTEM_AUTHENTICATION));
  }

  @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "User does not exist or is a non-native user!")
  public void testGenerateNativeUserResetTokenNotNativeUser() throws Exception {
    // Nonexistent corpUserCredentials
    when(_entityService.getLatestAspect(any(), eq(CORP_USER_CREDENTIALS_ASPECT_NAME))).thenReturn(null);

    _nativeUserService.generateNativeUserPasswordResetToken(USER_URN_STRING, SYSTEM_AUTHENTICATION);
  }

  @Test
  public void testGenerateNativeUserResetToken() throws Exception {
    CorpUserCredentials mockCorpUserCredentialsAspect = mock(CorpUserCredentials.class);
    when(_entityService.getLatestAspect(any(), eq(CORP_USER_CREDENTIALS_ASPECT_NAME))).thenReturn(
        mockCorpUserCredentialsAspect);
    when(mockCorpUserCredentialsAspect.hasSalt()).thenReturn(true);
    when(mockCorpUserCredentialsAspect.hasHashedPassword()).thenReturn(true);

    when(_secretService.encrypt(any())).thenReturn(ENCRYPTED_INVITE_TOKEN);

    _nativeUserService.generateNativeUserPasswordResetToken(USER_URN_STRING, SYSTEM_AUTHENTICATION);
    verify(_entityClient).ingestProposal(any(), any());
  }

  @Test
  public void testResetCorpUserCredentialsNullArguments() {
    assertThrows(() -> _nativeUserService.resetCorpUserCredentials(null, PASSWORD, RESET_TOKEN, SYSTEM_AUTHENTICATION));
    assertThrows(
        () -> _nativeUserService.resetCorpUserCredentials(USER_URN_STRING, null, RESET_TOKEN, SYSTEM_AUTHENTICATION));
    assertThrows(
        () -> _nativeUserService.resetCorpUserCredentials(USER_URN_STRING, PASSWORD, null, SYSTEM_AUTHENTICATION));
  }

  @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "User has not generated a password reset token!")
  public void testResetCorpUserCredentialsNoPasswordResetToken() throws Exception {
    CorpUserCredentials mockCorpUserCredentialsAspect = mock(CorpUserCredentials.class);
    when(_entityService.getLatestAspect(any(), eq(CORP_USER_CREDENTIALS_ASPECT_NAME))).thenReturn(
        mockCorpUserCredentialsAspect);
    when(mockCorpUserCredentialsAspect.hasSalt()).thenReturn(true);
    when(mockCorpUserCredentialsAspect.hasHashedPassword()).thenReturn(true);
    // No password reset token
    when(mockCorpUserCredentialsAspect.hasPasswordResetToken()).thenReturn(false);

    _nativeUserService.resetCorpUserCredentials(USER_URN_STRING, PASSWORD, RESET_TOKEN, SYSTEM_AUTHENTICATION);
  }

  @Test(expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = "Invalid reset token. Please ask your administrator to send you an updated link!")
  public void testResetCorpUserCredentialsBadResetToken() throws Exception {
    CorpUserCredentials mockCorpUserCredentialsAspect = mock(CorpUserCredentials.class);
    when(_entityService.getLatestAspect(any(), eq(CORP_USER_CREDENTIALS_ASPECT_NAME))).thenReturn(
        mockCorpUserCredentialsAspect);
    when(mockCorpUserCredentialsAspect.hasSalt()).thenReturn(true);
    when(mockCorpUserCredentialsAspect.hasHashedPassword()).thenReturn(true);
    when(mockCorpUserCredentialsAspect.hasPasswordResetToken()).thenReturn(true);
    when(mockCorpUserCredentialsAspect.getPasswordResetToken()).thenReturn(ENCRYPTED_RESET_TOKEN);
    when(mockCorpUserCredentialsAspect.hasPasswordResetTokenExpirationTimeMillis()).thenReturn(true);
    when(mockCorpUserCredentialsAspect.getPasswordResetTokenExpirationTimeMillis()).thenReturn(
        Instant.now().toEpochMilli());
    // Reset token won't match
    when(_secretService.decrypt(eq(ENCRYPTED_RESET_TOKEN))).thenReturn("badResetToken");

    _nativeUserService.resetCorpUserCredentials(USER_URN_STRING, PASSWORD, RESET_TOKEN, SYSTEM_AUTHENTICATION);
  }

  @Test(expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = "Reset token has expired! Please ask your administrator to create a new one")
  public void testResetCorpUserCredentialsExpiredResetToken() throws Exception {
    CorpUserCredentials mockCorpUserCredentialsAspect = mock(CorpUserCredentials.class);
    when(_entityService.getLatestAspect(any(), eq(CORP_USER_CREDENTIALS_ASPECT_NAME))).thenReturn(
        mockCorpUserCredentialsAspect);
    when(mockCorpUserCredentialsAspect.hasSalt()).thenReturn(true);
    when(mockCorpUserCredentialsAspect.hasHashedPassword()).thenReturn(true);
    when(mockCorpUserCredentialsAspect.hasPasswordResetToken()).thenReturn(true);
    when(mockCorpUserCredentialsAspect.getPasswordResetToken()).thenReturn(ENCRYPTED_RESET_TOKEN);
    when(mockCorpUserCredentialsAspect.hasPasswordResetTokenExpirationTimeMillis()).thenReturn(true);
    // Reset token expiration time will be before the system time when we run resetCorpUserCredentials
    when(mockCorpUserCredentialsAspect.getPasswordResetTokenExpirationTimeMillis()).thenReturn(0L);
    when(_secretService.decrypt(eq(ENCRYPTED_RESET_TOKEN))).thenReturn(RESET_TOKEN);

    _nativeUserService.resetCorpUserCredentials(USER_URN_STRING, PASSWORD, RESET_TOKEN, SYSTEM_AUTHENTICATION);
  }

  @Test
  public void testResetCorpUserCredentialsPasses() throws Exception {
    CorpUserCredentials mockCorpUserCredentialsAspect = mock(CorpUserCredentials.class);
    when(_entityService.getLatestAspect(any(), eq(CORP_USER_CREDENTIALS_ASPECT_NAME))).thenReturn(
        mockCorpUserCredentialsAspect);
    when(mockCorpUserCredentialsAspect.hasSalt()).thenReturn(true);
    when(mockCorpUserCredentialsAspect.hasHashedPassword()).thenReturn(true);
    when(mockCorpUserCredentialsAspect.hasPasswordResetToken()).thenReturn(true);
    when(mockCorpUserCredentialsAspect.getPasswordResetToken()).thenReturn(ENCRYPTED_RESET_TOKEN);
    when(mockCorpUserCredentialsAspect.hasPasswordResetTokenExpirationTimeMillis()).thenReturn(true);
    when(mockCorpUserCredentialsAspect.getPasswordResetTokenExpirationTimeMillis()).thenReturn(
        Instant.now().plusMillis(ONE_DAY_MILLIS).toEpochMilli());
    when(_secretService.decrypt(eq(ENCRYPTED_RESET_TOKEN))).thenReturn(RESET_TOKEN);
    when(_secretService.encrypt(any())).thenReturn(ENCRYPTED_SALT);

    _nativeUserService.resetCorpUserCredentials(USER_URN_STRING, PASSWORD, RESET_TOKEN, SYSTEM_AUTHENTICATION);
    verify(_entityClient).ingestProposal(any(), any());
  }

  @Test
  public void testDoesPasswordMatchNullArguments() {
    assertThrows(() -> _nativeUserService.doesPasswordMatch(null, PASSWORD));
    assertThrows(() -> _nativeUserService.doesPasswordMatch(USER_URN_STRING, null));
  }

  @Test
  public void testDoesPasswordMatchNoCorpUserCredentials() throws Exception {
    when(_entityService.getLatestAspect(any(), eq(CORP_USER_CREDENTIALS_ASPECT_NAME))).thenReturn(null);

    assertFalse(_nativeUserService.doesPasswordMatch(USER_URN_STRING, PASSWORD));
  }
}
