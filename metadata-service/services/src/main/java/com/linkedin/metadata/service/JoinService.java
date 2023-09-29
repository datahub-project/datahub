package com.linkedin.metadata.service;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

import static com.linkedin.metadata.Constants.*;


/**
 * This class is used to permit easy CRUD operations on a Query
 * Currently it supports creating and removing a Query.
 *
 * Note that no Authorization is performed within the service. The expectation
 * is that the caller has already verified the permissions of the active Actor.
 *
 */
@Slf4j
public class JoinService extends BaseService {

  public JoinService(@Nonnull EntityClient entityClient, @Nonnull Authentication systemAuthentication) {
    super(entityClient, systemAuthentication);
  }
  static final Set<String> ASPECTS_TO_RESOLVE = ImmutableSet.of(
          JOIN_KEY_ASPECT_NAME,
          JOIN_PROPERTIES_ASPECT_NAME,
          EDITABLE_JOIN_PROPERTIES_ASPECT_NAME,
          INSTITUTIONAL_MEMORY_ASPECT_NAME,
          OWNERSHIP_ASPECT_NAME,
          STATUS_ASPECT_NAME,
          GLOBAL_TAGS_ASPECT_NAME,
          GLOSSARY_TERMS_ASPECT_NAME
  );
  /**
   * Returns an instance of {@link EntityResponse} for the specified Join urn,
   * or null if one cannot be found.
   *
   * @param joinUrn the urn of the Query
   * @param authentication the authentication to use
   *
   * @return an instance of {@link EntityResponse} for the Join, null if it does not exist.
   */
  @Nullable
  public EntityResponse getJoinResponse(@Nonnull final Urn joinUrn, @Nonnull final Authentication authentication) {
    Objects.requireNonNull(joinUrn, "joinUrn must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");
    try {
      return this.entityClient.getV2(
          Constants.JOIN_ENTITY_NAME,
          joinUrn,
          ASPECTS_TO_RESOLVE,
          authentication
      );
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to retrieve Query with urn %s", joinUrn), e);
    }
  }
}