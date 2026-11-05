package com.linkedin.datahub.graphql.util;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.ResolvedAuditStamp;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.AspectType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EntityResponseUtils {

  private EntityResponseUtils() {}

  /**
   * Returns {@code entityResponse} when the caller can view the entity, and otherwise the response
   * a lookup of a urn that doesn't exist gets, so that mapping it can't reveal the entity.
   */
  @Nonnull
  public static EntityResponse viewableEntityResponse(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    if (context == null
        || AuthorizationUtils.canView(context.getOperationContext(), entityResponse.getUrn())) {
      return entityResponse;
    }
    return missingEntityResponse(
        context.getOperationContext().getEntityRegistry(), entityResponse.getUrn());
  }

  /**
   * The response a batch get returns for a urn that doesn't exist: only the key aspect, derived
   * from the urn, matching what EntityServiceImpl adds when the key aspect isn't stored.
   */
  @Nonnull
  public static EntityResponse missingEntityResponse(
      @Nonnull final EntityRegistry entityRegistry, @Nonnull final Urn urn) {
    final AspectSpec keySpec = entityRegistry.getEntitySpec(urn.getEntityType()).getKeyAspectSpec();
    final EnvelopedAspect keyAspect =
        new EnvelopedAspect()
            .setName(keySpec.getName())
            .setVersion(ASPECT_LATEST_VERSION)
            .setValue(new Aspect(EntityKeyUtils.convertUrnToEntityKey(urn, keySpec).data()))
            .setType(AspectType.VERSIONED)
            .setCreated(
                new AuditStamp()
                    .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))
                    .setTime(System.currentTimeMillis()));
    return new EntityResponse()
        .setUrn(urn)
        .setEntityName(urn.getEntityType())
        .setAspects(new EnvelopedAspectMap(Map.of(keySpec.getName(), keyAspect)));
  }

  @Nullable
  public static ResolvedAuditStamp extractAspectCreatedAuditStamp(
      final EntityResponse entityResponse, @Nonnull final String aspectName) {

    if (entityResponse == null) {
      log.warn(
          "Can't get created audit stamp for null entityResponse by aspectName {}", aspectName);
      return null;
    }

    Urn entityUrn = entityResponse.getUrn();

    if (!entityResponse.hasAspects()) {
      log.warn(
          "Can't get created audit stamp from entityResponse without aspects by aspectName {}. urn: {}",
          aspectName,
          entityUrn);
      return null;
    }

    EnvelopedAspectMap aspectMap = entityResponse.getAspects();

    if (!aspectMap.containsKey(aspectName)) {
      log.warn(
          "Can't get created audit stamp from entityResponse by aspectName {} as it doesn't contain this aspect. Urn: {}",
          aspectName,
          entityUrn);
      return null;
    }

    EnvelopedAspect aspect = aspectMap.get(aspectName);

    if (aspect == null) {
      log.warn(
          "Can't get created audit stamp from entityResponse by aspectName {} as this aspect is null. Urn: {}",
          aspectName,
          entityUrn);
      return null;
    }

    if (!aspect.hasCreated()) {
      log.warn(
          "Can't get created audit stamp from entityResponse as {} aspect doesn't have created audit stamp. Urn: {}",
          aspectName,
          entityUrn);
      return null;
    }

    ResolvedAuditStamp auditStamp = new ResolvedAuditStamp();
    if (CORP_USER_ENTITY_NAME.equals(aspect.getCreated().getActor().getEntityType())) {
      final CorpUser emptyCreatedUser = new CorpUser();
      emptyCreatedUser.setUrn(aspect.getCreated().getActor().toString());
      auditStamp.setActor(emptyCreatedUser);
    }
    auditStamp.setTime(aspect.getCreated().getTime());
    return auditStamp;
  }
}
