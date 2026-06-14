package com.datahub.authentication.user;

import static com.linkedin.metadata.Constants.*;

import com.datahub.authentication.AuthenticationConfiguration;
import com.datahub.authentication.LoginDenialReason;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpUserCredentials;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.CorpUserStatus;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import java.time.Instant;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Service responsible for creating, updating and authenticating native DataHub users. */
@Slf4j
@RequiredArgsConstructor
public class NativeUserService {
  private static final long DEFAULT_PASSWORD_RESET_TOKEN_EXPIRATION_MS = TimeUnit.DAYS.toMillis(1);
  private static final String FAILED_LOGIN_ATTEMPTS_FIELD_NAME = "failedLoginAttempts";
  private static final String LOGIN_LOCKOUT_EXPIRY_TIME_MILLIS_FIELD_NAME =
      "loginLockoutExpiryTimeMillis";

  private final EntityService<?> _entityService;
  private final EntityClient _entityClient;
  private final SecretService _secretService;
  private final AuthenticationConfiguration _authConfig;

  public void createNativeUser(
      @Nonnull OperationContext opContext,
      @Nonnull String userUrnString,
      @Nonnull String fullName,
      @Nonnull String email,
      String title,
      @Nonnull String password)
      throws Exception {
    Objects.requireNonNull(userUrnString, "userUrnSting must not be null!");
    Objects.requireNonNull(fullName, "fullName must not be null!");
    Objects.requireNonNull(email, "email must not be null!");
    Objects.requireNonNull(password, "password must not be null!");
    Objects.requireNonNull(
        opContext.getSessionAuthentication(), "authentication must not be null!");

    final Urn userUrn = Urn.createFromString(userUrnString);
    if (_entityService.exists(opContext, userUrn, true)
        // Should never fail these due to Controller level check, but just in case more usages get
        // put in
        || userUrn.toString().equals(SYSTEM_ACTOR)
        || userUrn.toString().equals(new CorpuserUrn(_authConfig.getSystemClientId()).toString())
        || userUrn.toString().equals(DATAHUB_ACTOR)
        || userUrn.toString().equals(UNKNOWN_ACTOR)) {
      throw new RuntimeException("This user already exists! Cannot create a new user.");
    }
    updateCorpUserInfo(opContext, userUrn, fullName, email, title);
    updateCorpUserStatus(opContext, userUrn);
    updateCorpUserCredentials(opContext, userUrn, password);
  }

  void updateCorpUserInfo(
      @Nonnull OperationContext opContext,
      @Nonnull Urn userUrn,
      @Nonnull String fullName,
      @Nonnull String email,
      String title)
      throws Exception {
    // Construct corpUserInfo
    final CorpUserInfo corpUserInfo = new CorpUserInfo();
    corpUserInfo.setFullName(fullName);
    corpUserInfo.setDisplayName(fullName);
    corpUserInfo.setEmail(email);
    if (title != null) {
      corpUserInfo.setTitle(title);
    }
    corpUserInfo.setActive(true);

    // Ingest corpUserInfo MCP
    final MetadataChangeProposal corpUserInfoProposal = new MetadataChangeProposal();
    corpUserInfoProposal.setEntityType(CORP_USER_ENTITY_NAME);
    corpUserInfoProposal.setEntityUrn(userUrn);
    corpUserInfoProposal.setAspectName(CORP_USER_INFO_ASPECT_NAME);
    corpUserInfoProposal.setAspect(GenericRecordUtils.serializeAspect(corpUserInfo));
    corpUserInfoProposal.setChangeType(ChangeType.UPSERT);
    _entityClient.ingestProposal(opContext, corpUserInfoProposal);
  }

  void updateCorpUserStatus(@Nonnull OperationContext opContext, @Nonnull Urn userUrn)
      throws Exception {
    // Construct corpUserStatus
    CorpUserStatus corpUserStatus = new CorpUserStatus();
    corpUserStatus.setStatus(CORP_USER_STATUS_ACTIVE);
    corpUserStatus.setLastModified(
        new AuditStamp()
            .setActor(Urn.createFromString(SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis()));

    // Ingest corpUserStatus MCP
    final MetadataChangeProposal corpUserStatusProposal = new MetadataChangeProposal();
    corpUserStatusProposal.setEntityType(CORP_USER_ENTITY_NAME);
    corpUserStatusProposal.setEntityUrn(userUrn);
    corpUserStatusProposal.setAspectName(CORP_USER_STATUS_ASPECT_NAME);
    corpUserStatusProposal.setAspect(GenericRecordUtils.serializeAspect(corpUserStatus));
    corpUserStatusProposal.setChangeType(ChangeType.UPSERT);
    _entityClient.ingestProposal(opContext, corpUserStatusProposal);
  }

  void updateCorpUserCredentials(
      @Nonnull OperationContext opContext, @Nonnull Urn userUrn, @Nonnull String password)
      throws Exception {
    // Construct corpUserCredentials
    CorpUserCredentials corpUserCredentials = new CorpUserCredentials();
    final byte[] salt = _secretService.generateSalt(SALT_TOKEN_LENGTH);
    String encryptedSalt = _secretService.encrypt(Base64.getEncoder().encodeToString(salt));
    corpUserCredentials.setSalt(encryptedSalt);
    String hashedPassword = _secretService.getHashedPassword(salt, password);
    corpUserCredentials.setHashedPassword(hashedPassword);
    clearFailedNativeLoginState(corpUserCredentials);

    ingestCorpUserCredentials(opContext, userUrn, corpUserCredentials);
  }

  public String generateNativeUserPasswordResetToken(
      @Nonnull OperationContext opContext, @Nonnull String userUrnString) throws Exception {
    Objects.requireNonNull(userUrnString, "userUrnString must not be null!");

    Urn userUrn = Urn.createFromString(userUrnString);

    CorpUserCredentials corpUserCredentials =
        (CorpUserCredentials)
            _entityService.getLatestAspect(opContext, userUrn, CORP_USER_CREDENTIALS_ASPECT_NAME);
    if (corpUserCredentials == null
        || !corpUserCredentials.hasSalt()
        || !corpUserCredentials.hasHashedPassword()) {
      throw new RuntimeException("User does not exist or is a non-native user!");
    }
    // Add reset token to CorpUserCredentials
    String passwordResetToken = _secretService.generateUrlSafeToken(PASSWORD_RESET_TOKEN_LENGTH);
    corpUserCredentials.setPasswordResetToken(_secretService.encrypt(passwordResetToken));

    long tokenExpirationMs = _authConfig.getPasswordResetTokenExpirationMs();
    if (tokenExpirationMs <= 0) {
      tokenExpirationMs = DEFAULT_PASSWORD_RESET_TOKEN_EXPIRATION_MS;
    }
    long expirationTime = Instant.now().plusMillis(tokenExpirationMs).toEpochMilli();
    corpUserCredentials.setPasswordResetTokenExpirationTimeMillis(expirationTime);

    ingestCorpUserCredentials(opContext, userUrn, corpUserCredentials);

    return passwordResetToken;
  }

  public void resetCorpUserCredentials(
      @Nonnull OperationContext opContext,
      @Nonnull String userUrnString,
      @Nonnull String password,
      @Nonnull String resetToken)
      throws Exception {
    Objects.requireNonNull(userUrnString, "userUrnString must not be null!");
    Objects.requireNonNull(password, "password must not be null!");
    Objects.requireNonNull(resetToken, "resetToken must not be null!");

    Urn userUrn = Urn.createFromString(userUrnString);

    CorpUserCredentials corpUserCredentials =
        (CorpUserCredentials)
            _entityService.getLatestAspect(opContext, userUrn, CORP_USER_CREDENTIALS_ASPECT_NAME);

    if (corpUserCredentials == null
        || !corpUserCredentials.hasSalt()
        || !corpUserCredentials.hasHashedPassword()) {
      throw new RuntimeException("User does not exist!");
    }

    if (!corpUserCredentials.hasPasswordResetToken()
        || !corpUserCredentials.hasPasswordResetTokenExpirationTimeMillis()
        || corpUserCredentials.getPasswordResetTokenExpirationTimeMillis() == null) {
      throw new RuntimeException("User has not generated a password reset token!");
    }

    if (!_secretService.decrypt(corpUserCredentials.getPasswordResetToken()).equals(resetToken)) {
      throw new RuntimeException(
          "Invalid reset token. Please ask your administrator to send you an updated link!");
    }

    long currentTimeMillis = Instant.now().toEpochMilli();
    if (currentTimeMillis > corpUserCredentials.getPasswordResetTokenExpirationTimeMillis()) {
      throw new RuntimeException(
          "Reset token has expired! Please ask your administrator to create a new one");
    }

    // Construct corpUserCredentials
    final byte[] salt = _secretService.generateSalt(SALT_TOKEN_LENGTH);
    String encryptedSalt = _secretService.encrypt(Base64.getEncoder().encodeToString(salt));
    corpUserCredentials.setSalt(encryptedSalt);
    String hashedPassword = _secretService.getHashedPassword(salt, password);
    corpUserCredentials.setHashedPassword(hashedPassword);
    clearFailedNativeLoginState(corpUserCredentials);

    ingestCorpUserCredentials(opContext, userUrn, corpUserCredentials);
  }

  public boolean doesPasswordMatch(
      @Nonnull OperationContext opContext, @Nonnull String userUrnString, @Nonnull String password)
      throws Exception {
    Objects.requireNonNull(userUrnString, "userUrnSting must not be null!");
    Objects.requireNonNull(password, "Password must not be null!");

    Urn userUrn = Urn.createFromString(userUrnString);
    CorpUserCredentials corpUserCredentials =
        (CorpUserCredentials)
            _entityService.getLatestAspect(opContext, userUrn, CORP_USER_CREDENTIALS_ASPECT_NAME);
    if (corpUserCredentials == null
        || !corpUserCredentials.hasSalt()
        || !corpUserCredentials.hasHashedPassword()) {
      return false;
    }

    String decryptedSalt = _secretService.decrypt(corpUserCredentials.getSalt());
    byte[] salt = Base64.getDecoder().decode(decryptedSalt);
    String storedHashedPassword = corpUserCredentials.getHashedPassword();
    String hashedPassword = _secretService.getHashedPassword(salt, password);
    return storedHashedPassword.equals(hashedPassword);
  }

  @Nonnull
  public Optional<LoginDenialReason> checkNativeLoginEligibility(
      @Nonnull OperationContext opContext, @Nonnull String userUrnString) throws Exception {
    Objects.requireNonNull(userUrnString, "userUrnSting must not be null!");
    if (!isFailedLoginLockoutEnabled()) {
      return Optional.empty();
    }

    final Urn userUrn = Urn.createFromString(userUrnString);
    final CorpUserCredentials corpUserCredentials = getCorpUserCredentials(opContext, userUrn);
    if (corpUserCredentials == null) {
      return Optional.empty();
    }

    final Long lockoutExpiry =
        getOptionalLong(corpUserCredentials, LOGIN_LOCKOUT_EXPIRY_TIME_MILLIS_FIELD_NAME);
    if (lockoutExpiry == null) {
      return Optional.empty();
    }

    final long now = Instant.now().toEpochMilli();
    if (lockoutExpiry > now) {
      return Optional.of(LoginDenialReason.TOO_MANY_FAILED_ATTEMPTS);
    }

    clearFailedNativeLoginState(corpUserCredentials);
    ingestCorpUserCredentials(opContext, userUrn, corpUserCredentials);
    return Optional.empty();
  }

  @Nonnull
  public LoginDenialReason recordFailedLoginAttempt(
      @Nonnull OperationContext opContext, @Nonnull String userUrnString) throws Exception {
    Objects.requireNonNull(userUrnString, "userUrnSting must not be null!");
    if (!isFailedLoginLockoutEnabled()) {
      return LoginDenialReason.INVALID_CREDENTIALS;
    }

    final Urn userUrn = Urn.createFromString(userUrnString);
    final CorpUserCredentials corpUserCredentials = getCorpUserCredentials(opContext, userUrn);
    if (corpUserCredentials == null) {
      return LoginDenialReason.INVALID_CREDENTIALS;
    }

    final int updatedAttempts = getFailedLoginAttempts(corpUserCredentials) + 1;
    corpUserCredentials.data().put(FAILED_LOGIN_ATTEMPTS_FIELD_NAME, updatedAttempts);

    LoginDenialReason denialReason = LoginDenialReason.INVALID_CREDENTIALS;
    if (updatedAttempts >= _authConfig.getMaxFailedLoginAttempts()) {
      corpUserCredentials
          .data()
          .put(
              LOGIN_LOCKOUT_EXPIRY_TIME_MILLIS_FIELD_NAME,
              Instant.now().toEpochMilli() + _authConfig.getFailedLoginLockoutDurationMs());
      denialReason = LoginDenialReason.TOO_MANY_FAILED_ATTEMPTS;
    }

    ingestCorpUserCredentials(opContext, userUrn, corpUserCredentials);
    return denialReason;
  }

  public void resetFailedLoginAttempts(
      @Nonnull OperationContext opContext, @Nonnull String userUrnString) throws Exception {
    Objects.requireNonNull(userUrnString, "userUrnSting must not be null!");

    final Urn userUrn = Urn.createFromString(userUrnString);
    final CorpUserCredentials corpUserCredentials = getCorpUserCredentials(opContext, userUrn);
    if (corpUserCredentials == null) {
      return;
    }

    final DataMap credentialsData = corpUserCredentials.data();
    if (credentialsData == null) {
      return;
    }

    if (!credentialsData.containsKey(FAILED_LOGIN_ATTEMPTS_FIELD_NAME)
        && !credentialsData.containsKey(LOGIN_LOCKOUT_EXPIRY_TIME_MILLIS_FIELD_NAME)) {
      return;
    }

    clearFailedNativeLoginState(corpUserCredentials);
    ingestCorpUserCredentials(opContext, userUrn, corpUserCredentials);
  }

  private boolean isFailedLoginLockoutEnabled() {
    return _authConfig.getMaxFailedLoginAttempts() > 0
        && _authConfig.getFailedLoginLockoutDurationMs() > 0;
  }

  private CorpUserCredentials getCorpUserCredentials(
      @Nonnull OperationContext opContext, @Nonnull Urn userUrn) throws Exception {
    return (CorpUserCredentials)
        _entityService.getLatestAspect(opContext, userUrn, CORP_USER_CREDENTIALS_ASPECT_NAME);
  }

  private void ingestCorpUserCredentials(
      @Nonnull OperationContext opContext,
      @Nonnull Urn userUrn,
      @Nonnull CorpUserCredentials corpUserCredentials)
      throws Exception {
    final MetadataChangeProposal corpUserCredentialsProposal = new MetadataChangeProposal();
    corpUserCredentialsProposal.setEntityType(CORP_USER_ENTITY_NAME);
    corpUserCredentialsProposal.setEntityUrn(userUrn);
    corpUserCredentialsProposal.setAspectName(CORP_USER_CREDENTIALS_ASPECT_NAME);
    corpUserCredentialsProposal.setAspect(GenericRecordUtils.serializeAspect(corpUserCredentials));
    corpUserCredentialsProposal.setChangeType(ChangeType.UPSERT);
    _entityClient.ingestProposal(opContext, corpUserCredentialsProposal);
  }

  private static int getFailedLoginAttempts(@Nonnull CorpUserCredentials corpUserCredentials) {
    final DataMap data = corpUserCredentials.data();
    if (data == null) {
      return 0;
    }
    final Object value = data.get(FAILED_LOGIN_ATTEMPTS_FIELD_NAME);
    return value instanceof Number ? ((Number) value).intValue() : 0;
  }

  private static Long getOptionalLong(
      @Nonnull CorpUserCredentials corpUserCredentials, @Nonnull String fieldName) {
    final DataMap data = corpUserCredentials.data();
    if (data == null) {
      return null;
    }
    final Object value = data.get(fieldName);
    return value instanceof Number ? ((Number) value).longValue() : null;
  }

  private static void clearFailedNativeLoginState(
      @Nonnull CorpUserCredentials corpUserCredentials) {
    final DataMap data = corpUserCredentials.data();
    if (data == null) {
      return;
    }
    data.remove(FAILED_LOGIN_ATTEMPTS_FIELD_NAME);
    data.remove(LOGIN_LOCKOUT_EXPIRY_TIME_MILLIS_FIELD_NAME);
  }
}
