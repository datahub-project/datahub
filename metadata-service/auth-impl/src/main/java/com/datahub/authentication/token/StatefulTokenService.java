package com.datahub.authentication.token;

import com.datahub.authentication.Actor;
import com.hazelcast.core.HazelcastInstance;
import com.linkedin.access.token.DataHubAccessTokenInfo;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.key.DataHubAccessTokenKey;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ReadPreference;
import java.util.*;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.ArrayUtils;

/**
 * Service responsible for generating JWT tokens & managing the associated metadata entities in GMS
 * for use within DataHub that are stored in the entity service so that we can list & revoke tokens
 * as needed.
 */
@Slf4j
public class StatefulTokenService extends StatelessTokenService {

  private final OperationContext systemOperationContext;
  private final EntityService<?> _entityService;
  private final TokenRevocationMap.State revokedTokens;
  private final String salt;

  public StatefulTokenService(
      @Nonnull final OperationContext systemOperationContext,
      @Nonnull final String signingKey,
      @Nonnull final String signingAlgorithm,
      @Nullable final String iss,
      @Nonnull final EntityService<?> entityService,
      @Nonnull final String salt,
      @Nullable final HazelcastInstance hazelcast) {
    super(systemOperationContext, signingKey, signingAlgorithm, iss);
    this.systemOperationContext = systemOperationContext;
    this._entityService = entityService;
    // MCE and partial Spring tests have no embedded node. GMS always does.
    this.revokedTokens =
        hazelcast == null ? TokenRevocationMap.local() : TokenRevocationMap.cluster(hazelcast);
    this.salt = Objects.requireNonNull(salt);
  }

  /**
   * Generates a JWT for an actor with a default expiration time.
   *
   * <p>Note that the caller of this method is expected to authorize the action of generating a
   * token.
   */
  @Override
  public String generateAccessToken(@Nonnull final TokenType type, @Nonnull final Actor actor) {
    throw new UnsupportedOperationException(
        "Please use generateToken(Token, Actor, String, String, String) endpoint "
            + "instead. Reason: StatefulTokenService requires that all tokens have a name & ownerUrn specified.");
  }

  @Nonnull
  public String generateAccessToken(
      @Nonnull final OperationContext opContext,
      @Nonnull final TokenType type,
      @Nonnull final Actor actor,
      @Nonnull final String name,
      final String description,
      final String actorUrn) {
    Date date = new Date();
    long timeMilli = date.getTime();
    return generateAccessToken(
        opContext, type, actor, DEFAULT_EXPIRES_IN_MS, timeMilli, name, description, actorUrn);
  }

  @Nonnull
  public String generateAccessToken(
      @Nonnull final OperationContext opContext,
      @Nonnull final TokenType type,
      @Nonnull final Actor actor,
      @Nullable final Long expiresInMs,
      @Nonnull final long createdAtInMs,
      @Nonnull final String tokenName,
      @Nullable final String tokenDescription,
      final String actorUrn) {

    Objects.requireNonNull(type);
    Objects.requireNonNull(actor);
    Objects.requireNonNull(tokenName);

    Map<String, Object> claims = new HashMap<>();
    // Only stateful token service generates v2 tokens.
    claims.put(TokenClaims.TOKEN_VERSION_CLAIM_NAME, String.valueOf(TokenVersion.TWO.numericValue));
    claims.put(TokenClaims.TOKEN_TYPE_CLAIM_NAME, type.toString());
    claims.put(TokenClaims.ACTOR_TYPE_CLAIM_NAME, actor.getType());
    claims.put(TokenClaims.ACTOR_ID_CLAIM_NAME, actor.getId());
    final String accessToken = super.generateAccessToken(actor.getId(), claims, expiresInMs);
    final String tokenHash = this.hash(accessToken);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();

    // Create the access token key --> use a hashed access token value as a unique id to ensure it's
    // not duplicated.
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
    if (expiresInMs != null) {
      value.setExpiresAt(createdAtInMs + expiresInMs);
    }
    proposal.setEntityType(Constants.ACCESS_TOKEN_ENTITY_NAME);
    proposal.setAspectName(Constants.ACCESS_TOKEN_INFO_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(value));
    proposal.setChangeType(ChangeType.UPSERT);

    log.info("About to ingest access token metadata {}", proposal);
    final AuditStamp auditStamp =
        AuditStampUtils.createDefaultAuditStamp().setActor(UrnUtils.getUrn(actorUrn));

    _entityService.ingestProposal(
        opContext,
        AspectsBatchImpl.builder()
            .mcps(List.of(proposal), auditStamp, opContext.getRetrieverContext())
            .build(opContext),
        false);

    // Published only after the primary commit. putIfAbsent so a revoke that already won is not
    // overwritten with not-revoked for the rest of the map TTL.
    revokedTokens.putIfAbsent(tokenHash, Boolean.FALSE);
    return accessToken;
  }

  @Nonnull
  @Override
  public TokenClaims validateAccessToken(@Nonnull String accessToken) throws TokenException {
    try {
      final TokenClaims tokenClaims = super.validateAccessToken(accessToken);
      if (tokenClaims.getTokenVersion().equals(TokenVersion.TWO)) {
        final String hash = hash(accessToken);
        if (isRevoked(hash)) {
          throw new TokenException("Failed to validate DataHub token: Token has been revoked");
        }
      }
      return tokenClaims;
    } catch (final TokenExpiredException e) {
      // delete entity
      this.revokeAccessToken(systemOperationContext, hash(accessToken));
      throw e;
    }
  }

  public Urn tokenUrnFromKey(String tokenHash) {
    return Urn.createFromTuple(Constants.ACCESS_TOKEN_ENTITY_NAME, tokenHash);
  }

  public void revokeAccessToken(OperationContext opContext, @Nonnull String hashedToken)
      throws TokenException {
    if (tokenExists(hashedToken)) {
      final Urn tokenUrn = tokenUrnFromKey(hashedToken);
      _entityService.deleteUrn(opContext, tokenUrn);
      revokedTokens.put(hashedToken, Boolean.TRUE);
      return;
    }
    revokedTokens.put(hashedToken, Boolean.TRUE);
    throw new TokenException("Access token no longer exists");
  }

  /**
   * Map value {@code true} means revoked. A miss is not revoked: reload {@code exists} from primary
   * so a lagging replica cannot be cached as a revocation.
   */
  private boolean isRevoked(@Nonnull String hashedToken) throws TokenException {
    return !tokenExists(hashedToken);
  }

  private boolean tokenExists(@Nonnull String hashedToken) throws TokenException {
    Boolean cached = revokedTokens.get(hashedToken);
    if (cached != null) {
      return !cached;
    }
    final boolean exists;
    try {
      exists =
          _entityService.exists(
              systemOperationContext.withReadPreference(ReadPreference.PRIMARY),
              tokenUrnFromKey(hashedToken),
              true);
    } catch (RuntimeException e) {
      throw new TokenException(
          "Failed to validate DataHub token: Unable to load token information from store", e);
    }
    revokedTokens.putIfAbsent(hashedToken, !exists);
    Boolean published = revokedTokens.get(hashedToken);
    return published == null ? exists : !published;
  }

  /** Hashes the input after salting it. */
  public String hash(String input) {
    final byte[] saltingKeyBytes = this.salt.getBytes();
    final byte[] inputBytes = input.getBytes();
    final byte[] concatBytes = ArrayUtils.addAll(inputBytes, saltingKeyBytes);
    final byte[] bytes = DigestUtils.sha256(concatBytes);
    return Base64.getEncoder().encodeToString(bytes);
  }
}
