package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is used to permit easy CRUD operations on a Query. Currently it supports creating and
 * removing a Query.
 *
 * <p>Note that no Authorization is performed within the service. The expectation is that the caller
 * has already verified the permissions of the active Actor.
 */
@Slf4j
public class ERModelRelationshipService extends BaseService {

  public ERModelRelationshipService(@Nonnull SystemEntityClient entityClient) {
    super(entityClient);
  }

  static final Set<String> ASPECTS_TO_RESOLVE =
      ImmutableSet.of(
          ER_MODEL_RELATIONSHIP_KEY_ASPECT_NAME,
          ER_MODEL_RELATIONSHIP_PROPERTIES_ASPECT_NAME,
          EDITABLE_ER_MODEL_RELATIONSHIP_PROPERTIES_ASPECT_NAME,
          INSTITUTIONAL_MEMORY_ASPECT_NAME,
          OWNERSHIP_ASPECT_NAME,
          STATUS_ASPECT_NAME,
          GLOBAL_TAGS_ASPECT_NAME,
          GLOSSARY_TERMS_ASPECT_NAME);

  /**
   * Returns an instance of {@link EntityResponse} for the specified ERModelRelationship urn, or
   * null if one cannot be found.
   *
   * @param ermodelrelationUrn the urn of the Query
   * @param authentication the authentication to use
   * @return an instance of {@link EntityResponse} for the ERModelRelationship, null if it does not
   *     exist.
   */
  @Nullable
  public EntityResponse getERModelRelationshipResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn ermodelrelationUrn) {
    Objects.requireNonNull(ermodelrelationUrn, "ermodelrelationUrn must not be null");
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");
    try {
      return this.entityClient.getV2(
          opContext,
          Constants.ER_MODEL_RELATIONSHIP_ENTITY_NAME,
          ermodelrelationUrn,
          ASPECTS_TO_RESOLVE);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve Query with urn %s", ermodelrelationUrn), e);
    }
  }
}
