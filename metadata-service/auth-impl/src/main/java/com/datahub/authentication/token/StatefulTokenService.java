package com.datahub.authentication.token;

import com.datahub.authentication.Actor;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.linkedin.access.token.DataHubAccessTokenInfo;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.DataHubAccessTokenKey;
import com.linkedin.metadata.resources.entity.AspectUtils;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.ArrayUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.datahub.authentication.token.TokenClaims.ACTOR_ID_CLAIM_NAME;
import static com.datahub.authentication.token.TokenClaims.ACTOR_TYPE_CLAIM_NAME;
import static com.datahub.authentication.token.TokenClaims.TOKEN_TYPE_CLAIM_NAME;
import static com.datahub.authentication.token.TokenClaims.TOKEN_VERSION_CLAIM_NAME;


/**
 * Service responsible for generating JWT tokens & managing the associated metadata entities in GMS for use within
 * DataHub that are stored in the entity service so that we can list & revoke tokens as needed.
 */
@Slf4j
public class StatefulTokenService extends StatelessTokenService {

  private final EntityService _entityService;
  private final LoadingCache<String, Boolean> _revokedTokenCache;
  private final String salt;
  private final MessageDigest sha256;

  public StatefulTokenService(@Nonnull final String signingKey, @Nonnull final String signingAlgorithm,
      @Nullable final String iss, @Nonnull final EntityService entityService, @Nonnull final String salt) {
    super(signingKey, signingAlgorithm, iss);
    this._entityService = entityService;
    this._revokedTokenCache = CacheBuilder.newBuilder()
        .maximumSize(10000)
        .expireAfterWrite(6, TimeUnit.HOURS)
        .build(new CacheLoader<String, Boolean>() {
          @Override
          public Boolean load(final String key) {
            final Urn accessUrn = Urn.createFromTuple(Constants.ACCESS_TOKEN_ENTITY_NAME, key);
            return !_entityService.exists(accessUrn);
          }
        });
    this.salt = salt;
    try {
      this.sha256 = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Unable to get SHA-256 algorithm.");
    }
  }

  /**
   * Generates a JWT for an actor with a default expiration time.
   *
   * Note that the caller of this method is expected to authorize the action of generating a token.
   *
   */
  @Override
  public String generateAccessToken(@Nonnull final TokenType type, @Nonnull final Actor actor) {
    throw new UnsupportedOperationException("Please use generateToken(Token, Actor, String, String, String) endpoint "
        + "instead. Reason: StatefulTokenService requires that all tokens have a name & ownerUrn specified.");
  }

  @Nonnull
  public String generateAccessToken(@Nonnull final TokenType type, @Nonnull final Actor actor,
      @Nonnull final String name, final String description, final String actorUrn) {
    Date date = new Date();
    long timeMilli = date.getTime();
    return generateAccessToken(type, actor, DEFAULT_EXPIRES_IN_MS, timeMilli, name, description, actorUrn);
  }

  @Nonnull
  public String generateAccessToken(@Nonnull final TokenType type, @Nonnull final Actor actor,
      @Nonnull final long expiresInMs, @Nonnull final long createdAtInMs, @Nonnull final String tokenName,
      @Nullable final String tokenDescription, final String actorUrn) {

    Objects.requireNonNull(type);
    Objects.requireNonNull(actor);
    Objects.requireNonNull(tokenName);
    Map<String, Object> claims = new HashMap<>();
    // Only stateful token service generates v2 tokens.
    claims.put(TOKEN_VERSION_CLAIM_NAME, String.valueOf(TokenVersion.TWO.numericValue));
    claims.put(TOKEN_TYPE_CLAIM_NAME, type.toString());
    claims.put(ACTOR_TYPE_CLAIM_NAME, actor.getType());
    claims.put(ACTOR_ID_CLAIM_NAME, actor.getId());
    final String accessToken = super.generateAccessToken(actor.getId(), claims, expiresInMs);
    final String tokenHash = this.hash(accessToken);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();

    // Create the access token key --> use a hashed access token value as a unique id to ensure it's not duplicated.
    final DataHubAccessTokenKey key = new DataHubAccessTokenKey();
    key.setId(tokenHash);
    proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));

    // Create the secret value.
    final DataHubAccessTokenInfo value = new DataHubAccessTokenInfo();
    value.setName(tokenName);
    if (tokenDescription != null) {
      value.setDescription(tokenDescription);
    }
    value.setActorUrn(UrnUtils.getUrn(actor.toUrnStr()));
    value.setOwnerUrn(UrnUtils.getUrn(actorUrn));
    value.setCreatedAt(createdAtInMs);
    value.setExpiresAt(createdAtInMs + expiresInMs);

    proposal.setEntityType(Constants.ACCESS_TOKEN_ENTITY_NAME);
    proposal.setAspectName(Constants.ACCESS_TOKEN_INFO_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(value));
    proposal.setChangeType(ChangeType.UPSERT);

    log.info("About to ingest access token metadata {}", proposal);
    final AuditStamp auditStamp = AuditStampUtils.createDefaultAuditStamp().setActor(UrnUtils.getUrn(actorUrn));

    // Need this to write key aspect
    final List<MetadataChangeProposal> additionalChanges = AspectUtils.getAdditionalChanges(proposal, _entityService);

    _entityService.ingestProposal(proposal, auditStamp);
    additionalChanges.forEach(mcp -> _entityService.ingestProposal(mcp, auditStamp));

    return accessToken;
  }

  @Nonnull
  @Override
  public TokenClaims validateAccessToken(@Nonnull String accessToken) throws TokenException {
    try {
      final TokenClaims tokenClaims = super.validateAccessToken(accessToken);
      if (tokenClaims.getTokenVersion().equals(TokenVersion.TWO)) {
        final String hash = hash(accessToken);
        if (_revokedTokenCache.get(hash)) {
          throw new TokenException("Failed to validate DataHub token: Token has been revoked");
        }
      }
      return tokenClaims;
    } catch (final TokenExpiredException e) {
      // delete entity
      this.revokeAccessToken(hash(accessToken));
      throw e;
    } catch (final ExecutionException e) {
      throw new TokenException("Failed to validate DataHub token: Unable to load token information from store");
    }
  }

  public void revokeAccessToken(@Nonnull String hashedToken) throws TokenException {
    try {
      if (!_revokedTokenCache.get(hashedToken)) {
        final Urn tokenUrn = Urn.createFromTuple(Constants.ACCESS_TOKEN_ENTITY_NAME, hashedToken);
        _entityService.deleteUrn(tokenUrn);
        _revokedTokenCache.put(hashedToken, true);
        return;
      }
    } catch (ExecutionException e) {
      throw new TokenException("Failed to validate DataHub token from cache");
    }
    throw new TokenException("Access token no longer exists");
  }

  /**
   * Hashes the input after salting it.
   */
  public String hash(String input) {
    final byte[] saltingKeyBytes = this.salt.getBytes();
    final byte[] inputBytes = input.getBytes();
    final byte[] concatBytes = ArrayUtils.addAll(inputBytes, saltingKeyBytes);
    final byte[] bytes = sha256.digest(concatBytes);
    return Base64.getEncoder().encodeToString(bytes);
  }
}
