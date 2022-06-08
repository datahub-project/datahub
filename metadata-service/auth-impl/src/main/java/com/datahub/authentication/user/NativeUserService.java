package com.datahub.authentication.user;

import com.datahub.authentication.Authentication;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpUserCredentials;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.CorpUserStatus;
import com.linkedin.identity.InviteToken;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.secret.SecretService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Base64;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;


/**
 * Service responsible for creating, updating and authenticating native DataHub users.
 */
@Slf4j
public class NativeUserService {
  private static final int LOWERCASE_ASCII_START = 97;
  private static final int LOWERCASE_ASCII_END = 122;
  private static final int INVITE_TOKEN_LENGTH = 32;
  private static final int SALT_TOKEN_LENGTH = 16;
  private static final int PASSWORD_RESET_TOKEN_LENGTH = 32;
  private static final String HASHING_ALGORITHM = "SHA-256";
  private static final long ONE_DAY_MILLIS = TimeUnit.DAYS.toMillis(1);

  private final EntityService _entityService;
  private final EntityClient _entityClient;
  private final SecretService _secretService;
  private final SecureRandom _secureRandom;
  private final MessageDigest _messageDigest;

  public NativeUserService(@Nonnull EntityService entityService, @Nonnull EntityClient entityClient, @Nonnull SecretService secretService)
      throws Exception {
    Objects.requireNonNull(entityService, "entityService must not be null!");
    Objects.requireNonNull(entityClient, "entityClient must not be null!");
    Objects.requireNonNull(secretService, "secretService must not be null!");

    _entityService = entityService;
    _entityClient = entityClient;
    _secretService = secretService;
    _secureRandom = new SecureRandom();
    _messageDigest = MessageDigest.getInstance(HASHING_ALGORITHM);
  }

  public void createNativeUser(@Nonnull String userUrnString, @Nonnull String fullName, @Nonnull String email,
      @Nonnull String title, @Nonnull String password, @Nonnull String inviteToken, Authentication authentication)
      throws Exception {
    Objects.requireNonNull(userUrnString, "userUrnSting must not be null!");
    Objects.requireNonNull(fullName, "fullName must not be null!");
    Objects.requireNonNull(email, "email must not be null!");
    Objects.requireNonNull(title, "title must not be null!");
    Objects.requireNonNull(password, "password must not be null!");
    Objects.requireNonNull(inviteToken, "inviteToken must not be null!");

    InviteToken inviteTokenAspect =
        (InviteToken) _entityService.getLatestAspect(Urn.createFromString(GLOBAL_INVITE_TOKEN),
            INVITE_TOKEN_ASPECT_NAME);
    if (inviteTokenAspect == null || !inviteTokenAspect.hasToken() || !_secretService.decrypt(
        inviteTokenAspect.getToken()).equals(inviteToken)) {
      throw new RuntimeException("Invalid sign-up token. Please ask your administrator to send you an updated link!");
    }

    Urn userUrn = Urn.createFromString(userUrnString);
    if (_entityService.exists(userUrn)) {
      throw new RuntimeException("This user already exists! Cannot create a new user.");
    }
    updateCorpUserInfo(userUrn, fullName, email, title, authentication);
    updateCorpUserStatus(userUrn, authentication);
    updateCorpUserCredentials(userUrn, password, authentication);
  }

  void updateCorpUserInfo(@Nonnull Urn userUrn, @Nonnull String fullName, @Nonnull String email, @Nonnull String title,
      Authentication authentication) throws Exception {
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
    _entityClient.ingestProposal(corpUserInfoProposal, authentication);
  }

  void updateCorpUserStatus(@Nonnull Urn userUrn, Authentication authentication) throws Exception {
    // Construct corpUserStatus
    CorpUserStatus corpUserStatus = new CorpUserStatus();
    corpUserStatus.setStatus(CORP_USER_STATUS_ACTIVE);
    corpUserStatus.setLastModified(
        new AuditStamp().setActor(Urn.createFromString(SYSTEM_ACTOR)).setTime(System.currentTimeMillis()));

    // Ingest corpUserStatus MCP
    final MetadataChangeProposal corpUserStatusProposal = new MetadataChangeProposal();
    corpUserStatusProposal.setEntityType(CORP_USER_ENTITY_NAME);
    corpUserStatusProposal.setEntityUrn(userUrn);
    corpUserStatusProposal.setAspectName(CORP_USER_STATUS_ASPECT_NAME);
    corpUserStatusProposal.setAspect(GenericRecordUtils.serializeAspect(corpUserStatus));
    corpUserStatusProposal.setChangeType(ChangeType.UPSERT);
    _entityClient.ingestProposal(corpUserStatusProposal, authentication);
  }

  void updateCorpUserCredentials(@Nonnull Urn userUrn, @Nonnull String password,
      Authentication authentication) throws Exception {
    // Construct corpUserCredentials
    CorpUserCredentials corpUserCredentials = new CorpUserCredentials();
    final byte[] salt = getRandomBytes(SALT_TOKEN_LENGTH);
    String encryptedSalt = _secretService.encrypt(Base64.getEncoder().encodeToString(salt));
    corpUserCredentials.setSalt(encryptedSalt);
    String hashedPassword = getHashedPassword(salt, password);
    corpUserCredentials.setHashedPassword(hashedPassword);

    // Ingest corpUserCredentials MCP
    final MetadataChangeProposal corpUserCredentialsProposal = new MetadataChangeProposal();
    corpUserCredentialsProposal.setEntityType(CORP_USER_ENTITY_NAME);
    corpUserCredentialsProposal.setEntityUrn(userUrn);
    corpUserCredentialsProposal.setAspectName(CORP_USER_CREDENTIALS_ASPECT_NAME);
    corpUserCredentialsProposal.setAspect(GenericRecordUtils.serializeAspect(corpUserCredentials));
    corpUserCredentialsProposal.setChangeType(ChangeType.UPSERT);
    _entityClient.ingestProposal(corpUserCredentialsProposal, authentication);
  }

  public String generateNativeUserInviteToken(Authentication authentication) throws Exception {
    // Construct inviteToken
    InviteToken inviteToken = new InviteToken();
    String token = generateRandomLowercaseToken(INVITE_TOKEN_LENGTH);
    inviteToken.setToken(_secretService.encrypt(token));

    // Ingest corpUserCredentials MCP
    final MetadataChangeProposal inviteTokenProposal = new MetadataChangeProposal();
    inviteTokenProposal.setEntityType(INVITE_TOKEN_ENTITY_NAME);
    inviteTokenProposal.setEntityUrn(Urn.createFromString(GLOBAL_INVITE_TOKEN));
    inviteTokenProposal.setAspectName(INVITE_TOKEN_ASPECT_NAME);
    inviteTokenProposal.setAspect(GenericRecordUtils.serializeAspect(inviteToken));
    inviteTokenProposal.setChangeType(ChangeType.UPSERT);
    _entityClient.ingestProposal(inviteTokenProposal, authentication);

    return token;
  }

  public String getNativeUserInviteToken(Authentication authentication) throws Exception {
    InviteToken inviteToken = (InviteToken) _entityService.getLatestAspect(Urn.createFromString(GLOBAL_INVITE_TOKEN),
        INVITE_TOKEN_ASPECT_NAME);
    if (inviteToken == null || !inviteToken.hasToken()) {
      return generateNativeUserInviteToken(authentication);
    }
    return _secretService.decrypt(inviteToken.getToken());
  }

  public String generateNativeUserPasswordResetToken(@Nonnull String userUrnString,
      Authentication authentication) throws Exception {
    Objects.requireNonNull(userUrnString, "userUrnString must not be null!");

    Urn userUrn = Urn.createFromString(userUrnString);

    CorpUserCredentials corpUserCredentials =
        (CorpUserCredentials) _entityService.getLatestAspect(userUrn, CORP_USER_CREDENTIALS_ASPECT_NAME);
    if (corpUserCredentials == null || !corpUserCredentials.hasSalt() || !corpUserCredentials.hasHashedPassword()) {
      throw new RuntimeException("User does not exist or is a non-native user!");
    }
    // Add reset token to CorpUserCredentials
    String passwordResetToken = generateRandomLowercaseToken(PASSWORD_RESET_TOKEN_LENGTH);
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
    _entityClient.ingestProposal(corpUserCredentialsProposal, authentication);

    return passwordResetToken;
  }

  public void resetCorpUserCredentials(@Nonnull String userUrnString, @Nonnull String password,
      @Nonnull String resetToken, Authentication authentication) throws Exception {
    Objects.requireNonNull(userUrnString, "userUrnString must not be null!");
    Objects.requireNonNull(password, "password must not be null!");
    Objects.requireNonNull(resetToken, "resetToken must not be null!");

    Urn userUrn = Urn.createFromString(userUrnString);

    CorpUserCredentials corpUserCredentials =
        (CorpUserCredentials) _entityService.getLatestAspect(userUrn, CORP_USER_CREDENTIALS_ASPECT_NAME);

    if (corpUserCredentials == null || !corpUserCredentials.hasSalt() || !corpUserCredentials.hasHashedPassword()) {
      throw new RuntimeException("User does not exist!");
    }

    if (!corpUserCredentials.hasPasswordResetToken()
        || !corpUserCredentials.hasPasswordResetTokenExpirationTimeMillis()
        || corpUserCredentials.getPasswordResetTokenExpirationTimeMillis() == null) {
      throw new RuntimeException("User has not generated a password reset token!");
    }

    if (!_secretService.decrypt(
        corpUserCredentials.getPasswordResetToken()).equals(resetToken)) {
      throw new RuntimeException("Invalid reset token. Please ask your administrator to send you an updated link!");
    }

    long currentTimeMillis = Instant.now().toEpochMilli();
    if (currentTimeMillis > corpUserCredentials.getPasswordResetTokenExpirationTimeMillis()) {
      throw new RuntimeException("Reset token has expired! Please ask your administrator to create a new one");
    }

    // Construct corpUserCredentials
    final byte[] salt = getRandomBytes(SALT_TOKEN_LENGTH);
    String encryptedSalt = _secretService.encrypt(Base64.getEncoder().encodeToString(salt));
    corpUserCredentials.setSalt(encryptedSalt);
    String hashedPassword = getHashedPassword(salt, password);
    corpUserCredentials.setHashedPassword(hashedPassword);

    // Ingest corpUserCredentials MCP
    final MetadataChangeProposal corpUserCredentialsProposal = new MetadataChangeProposal();
    corpUserCredentialsProposal.setEntityType(CORP_USER_ENTITY_NAME);
    corpUserCredentialsProposal.setEntityUrn(userUrn);
    corpUserCredentialsProposal.setAspectName(CORP_USER_CREDENTIALS_ASPECT_NAME);
    corpUserCredentialsProposal.setAspect(GenericRecordUtils.serializeAspect(corpUserCredentials));
    corpUserCredentialsProposal.setChangeType(ChangeType.UPSERT);
    _entityClient.ingestProposal(corpUserCredentialsProposal, authentication);
  }

  byte[] getRandomBytes(int length) {
    byte[] randomBytes = new byte[length];
    _secureRandom.nextBytes(randomBytes);
    return randomBytes;
  }

  String generateRandomLowercaseToken(int length) {
    return _secureRandom.ints(length, LOWERCASE_ASCII_START, LOWERCASE_ASCII_END + 1)
        .mapToObj(i -> String.valueOf((char) i))
        .collect(Collectors.joining());
  }

  byte[] saltPassword(@Nonnull byte[] salt, @Nonnull String password) throws IOException {
    byte[] passwordBytes = password.getBytes();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    byteArrayOutputStream.write(salt);
    byteArrayOutputStream.write(passwordBytes);
    return byteArrayOutputStream.toByteArray();
  }

  public String getHashedPassword(@Nonnull byte[] salt, @Nonnull String password) throws IOException {
    byte[] saltedPassword = saltPassword(salt, password);
    byte[] hashedPassword = _messageDigest.digest(saltedPassword);
    return Base64.getEncoder().encodeToString(hashedPassword);
  }

  public boolean doesPasswordMatch(@Nonnull String userUrnString, @Nonnull String password) throws Exception {
    Objects.requireNonNull(userUrnString, "userUrnSting must not be null!");
    Objects.requireNonNull(password, "Password must not be null!");

    Urn userUrn = Urn.createFromString(userUrnString);
    CorpUserCredentials corpUserCredentials =
        (CorpUserCredentials) _entityService.getLatestAspect(userUrn, CORP_USER_CREDENTIALS_ASPECT_NAME);
    if (corpUserCredentials == null || !corpUserCredentials.hasSalt() || !corpUserCredentials.hasHashedPassword()) {
      return false;
    }

    String decryptedSalt = _secretService.decrypt(corpUserCredentials.getSalt());
    byte[] salt = Base64.getDecoder().decode(decryptedSalt);
    String storedHashedPassword = corpUserCredentials.getHashedPassword();
    String hashedPassword = getHashedPassword(salt, password);
    return storedHashedPassword.equals(hashedPassword);
  }
}
