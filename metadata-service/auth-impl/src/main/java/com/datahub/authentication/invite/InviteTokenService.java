package com.datahub.authentication.invite;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.key.InviteTokenKey;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.secret.SecretService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;


@Slf4j
public class InviteTokenService {
  private static final String HASHING_ALGORITHM = "SHA-256";
  private static final String ROLE_FIELD_NAME = "role";
  private static final String HAS_ROLE_FIELD_NAME = "hasRole";
  private final EntityClient _entityClient;
  private final SecretService _secretService;
  private final MessageDigest _messageDigest;
  private final Base64.Encoder _encoder;

  public InviteTokenService(@Nonnull EntityClient entityClient, @Nonnull SecretService secretService) throws Exception {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
    _secretService = Objects.requireNonNull(secretService, "secretService must not be null");
    _messageDigest = MessageDigest.getInstance(HASHING_ALGORITHM);
    _encoder = Base64.getEncoder();
  }

  public Urn getInviteTokenUrn(@Nonnull final String inviteTokenStr) throws URISyntaxException {
    byte[] hashedInviteTokenBytes = _messageDigest.digest(inviteTokenStr.getBytes());
    String hashedInviteTokenStr = _encoder.encodeToString(hashedInviteTokenBytes);
    String inviteTokenUrnStr = String.format("urn:li:inviteToken:%s", hashedInviteTokenStr);
    return Urn.createFromString(inviteTokenUrnStr);
  }

  public boolean isInviteTokenValid(@Nonnull final Urn inviteTokenUrn, @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    return _entityClient.exists(inviteTokenUrn, authentication);
  }

  public Optional<Urn> getRoleUrnFromInviteToken(@Nonnull final Urn inviteTokenUrn,
      @Nonnull final Authentication authentication) throws URISyntaxException, RemoteInvocationException {
    final EntityResponse inviteTokenEntity =
        _entityClient.getV2(INVITE_TOKEN_ENTITY_NAME, inviteTokenUrn, Collections.singleton(INVITE_TOKEN_ASPECT_NAME),
            authentication);
    if (inviteTokenEntity == null) {
      return Optional.empty();
    }

    final EnvelopedAspectMap aspectMap = inviteTokenEntity.getAspects();
    // If invite token aspect is not present, create a new one. Otherwise, return existing one.
    if (!aspectMap.containsKey(INVITE_TOKEN_ASPECT_NAME)) {
      return Optional.empty();
    }

    com.linkedin.identity.InviteToken inviteToken =
        new com.linkedin.identity.InviteToken(aspectMap.get(INVITE_TOKEN_ASPECT_NAME).getValue().data());
    return inviteToken.hasRole() ? Optional.of(inviteToken.getRole()) : Optional.empty();
  }

  @Nonnull
  public String getInviteToken(@Nonnull final Optional<Urn> optionalRoleUrn, boolean regenerate,
      @Nonnull final Authentication authentication) throws Exception {
    final Filter inviteTokenFilter;
    if (optionalRoleUrn.isPresent()) {
      final Urn roleUrn = optionalRoleUrn.get();
      if (!_entityClient.exists(roleUrn, authentication)) {
        throw new RuntimeException(String.format("Role %s does not exist", roleUrn));
      }
      inviteTokenFilter = createInviteTokenFilter(roleUrn);
    } else {
      inviteTokenFilter = createInviteTokenFilter();
    }

    final SearchResult searchResult =
        _entityClient.filter(INVITE_TOKEN_ENTITY_NAME, inviteTokenFilter, null, 0, 10, authentication);
    // If there are no entities in the result, create a new invite token.
    if (regenerate || searchResult.getEntities().isEmpty()) {
      return createInviteToken(optionalRoleUrn, searchResult, authentication);
    }

    final SearchEntity searchEntity = searchResult.getEntities().get(0);
    final Urn inviteTokenUrn = searchEntity.getEntity();
    final EntityResponse inviteTokenEntity =
        _entityClient.getV2(INVITE_TOKEN_ENTITY_NAME, inviteTokenUrn, Collections.singleton(INVITE_TOKEN_ASPECT_NAME),
            authentication);

    if (inviteTokenEntity == null) {
      throw new RuntimeException(String.format("Invite token %s does not exist", inviteTokenUrn));
    }

    final EnvelopedAspectMap aspectMap = inviteTokenEntity.getAspects();
    // If invite token aspect is not present, create a new one. Otherwise, return existing one.
    if (!aspectMap.containsKey(INVITE_TOKEN_ASPECT_NAME)) {
      return createInviteToken(optionalRoleUrn, searchResult, authentication);
    }

    com.linkedin.identity.InviteToken inviteToken =
        new com.linkedin.identity.InviteToken(aspectMap.get(INVITE_TOKEN_ASPECT_NAME).getValue().data());
    return _secretService.decrypt(inviteToken.getToken());
  }

  private Filter createInviteTokenFilter() {
    final Filter filter = new Filter();
    final ConjunctiveCriterionArray disjunction = new ConjunctiveCriterionArray();
    final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
    final CriterionArray andCriterion = new CriterionArray();

    final Criterion roleCriterion = new Criterion();
    roleCriterion.setField(HAS_ROLE_FIELD_NAME);
    roleCriterion.setValue("false");
    roleCriterion.setCondition(Condition.EQUAL);

    andCriterion.add(roleCriterion);
    conjunction.setAnd(andCriterion);
    disjunction.add(conjunction);
    filter.setOr(disjunction);

    return filter;
  }

  private Filter createInviteTokenFilter(Urn roleUrn) {
    final Filter filter = new Filter();
    final ConjunctiveCriterionArray disjunction = new ConjunctiveCriterionArray();
    final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
    final CriterionArray andCriterion = new CriterionArray();

    final Criterion roleCriterion = new Criterion();
    roleCriterion.setField(ROLE_FIELD_NAME);
    roleCriterion.setValue(roleUrn.toString());
    roleCriterion.setCondition(Condition.EQUAL);

    andCriterion.add(roleCriterion);
    conjunction.setAnd(andCriterion);
    disjunction.add(conjunction);
    filter.setOr(disjunction);

    return filter;
  }

  @Nonnull
  private String createInviteToken(@Nonnull final Optional<Urn> roleUrn, @Nonnull final SearchResult searchResult,
      @Nonnull final Authentication authentication) throws Exception {
    deleteExistingInviteTokens(searchResult, authentication);

    String inviteTokenStr = UUID.randomUUID().toString();
    byte[] hashedInviteTokenBytes = _messageDigest.digest(inviteTokenStr.getBytes());
    String hashedInviteTokenStr = _encoder.encodeToString(hashedInviteTokenBytes);
    InviteTokenKey inviteTokenKey = new InviteTokenKey();
    inviteTokenKey.setId(hashedInviteTokenStr);
    com.linkedin.identity.InviteToken inviteTokenAspect =
        new com.linkedin.identity.InviteToken().setToken(_secretService.encrypt(inviteTokenStr));
    roleUrn.ifPresent(inviteTokenAspect::setRole);

    // Ingest inviteToken MCP
    final MetadataChangeProposal inviteTokenProposal = new MetadataChangeProposal();
    inviteTokenProposal.setEntityType(INVITE_TOKEN_ENTITY_NAME);
    inviteTokenProposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(inviteTokenKey));
    inviteTokenProposal.setAspectName(INVITE_TOKEN_ASPECT_NAME);
    inviteTokenProposal.setAspect(GenericRecordUtils.serializeAspect(inviteTokenAspect));
    inviteTokenProposal.setChangeType(ChangeType.UPSERT);
    _entityClient.ingestProposal(inviteTokenProposal, authentication);

    return inviteTokenStr;
  }

  private void deleteExistingInviteTokens(@Nonnull final SearchResult searchResult,
      @Nonnull final Authentication authentication) {
    searchResult.getEntities().forEach(entity -> {
      try {
        _entityClient.deleteEntity(entity.getEntity(), authentication);
      } catch (RemoteInvocationException e) {
        log.error(String.format("Failed to delete invite token entity %s", entity.getEntity()), e);
      }
    });
  }
}
