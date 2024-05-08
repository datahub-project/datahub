package com.datahub.authentication.user;

import static com.linkedin.metadata.Constants.*;

import com.datahub.authentication.AuthenticationConfiguration;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
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
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Service responsible for creating, updating and authenticating native DataHub users. */
@Slf4j
@RequiredArgsConstructor
public class NativeUserService {
  private static final long ONE_DAY_MILLIS = TimeUnit.DAYS.toMillis(1);

  private final EntityService<?> _entityService;
  private final EntityClient _entityClient;
  private final SecretService _secretService;
  private final AuthenticationConfiguration _authConfig;

  public void createNativeUser(
      @Nonnull OperationContext opContext,
      @Nonnull String userUrnString,
      @Nonnull String fullName,
      @Nonnull String email,
      @Nonnull String title,
      @Nonnull String password)
      throws Exception {
    Objects.requireNonNull(userUrnString, "userUrnSting must not be null!");
    Objects.requireNonNull(fullName, "fullName must not be null!");
    Objects.requireNonNull(email, "email must not be null!");
    Objects.requireNonNull(title, "title must not be null!");
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
      @Nonnull String title)
      throws Exception {
    // Construct corpUserInfo
    final CorpUserInfo corpUserInfo = new CorpUserInfo();
    corpUserInfo.setFullName(fullName);
    corpUserInfo.setDisplayName(fullName);
    corpUserInfo.setEmail(email);
    corpUserInfo.setTitle(title);
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

    // Ingest corpUserCredentials MCP
    final MetadataChangeProposal corpUserCredentialsProposal = new MetadataChangeProposal();
    corpUserCredentialsProposal.setEntityType(CORP_USER_ENTITY_NAME);
    corpUserCredentialsProposal.setEntityUrn(userUrn);
    corpUserCredentialsProposal.setAspectName(CORP_USER_CREDENTIALS_ASPECT_NAME);
    corpUserCredentialsProposal.setAspect(GenericRecordUtils.serializeAspect(corpUserCredentials));
    corpUserCredentialsProposal.setChangeType(ChangeType.UPSERT);
    _entityClient.ingestProposal(opContext, corpUserCredentialsProposal);
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

    long expirationTime = Instant.now().plusMillis(ONE_DAY_MILLIS).toEpochMilli();
    corpUserCredentials.setPasswordResetTokenExpirationTimeMillis(expirationTime);

    // Ingest CorpUserCredentials MCP
    final MetadataChangeProposal corpUserCredentialsProposal = new MetadataChangeProposal();
    corpUserCredentialsProposal.setEntityType(CORP_USER_ENTITY_NAME);
    corpUserCredentialsProposal.setEntityUrn(userUrn);
    corpUserCredentialsProposal.setAspectName(CORP_USER_CREDENTIALS_ASPECT_NAME);
    corpUserCredentialsProposal.setAspect(GenericRecordUtils.serializeAspect(corpUserCredentials));
    corpUserCredentialsProposal.setChangeType(ChangeType.UPSERT);
    _entityClient.ingestProposal(opContext, corpUserCredentialsProposal);

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

    // Ingest corpUserCredentials MCP
    final MetadataChangeProposal corpUserCredentialsProposal = new MetadataChangeProposal();
    corpUserCredentialsProposal.setEntityType(CORP_USER_ENTITY_NAME);
    corpUserCredentialsProposal.setEntityUrn(userUrn);
    corpUserCredentialsProposal.setAspectName(CORP_USER_CREDENTIALS_ASPECT_NAME);
    corpUserCredentialsProposal.setAspect(GenericRecordUtils.serializeAspect(corpUserCredentials));
    corpUserCredentialsProposal.setChangeType(ChangeType.UPSERT);
    _entityClient.ingestProposal(opContext, corpUserCredentialsProposal);
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
}
