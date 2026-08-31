package com.linkedin.datahub.graphql.util;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.ResolvedAuditStamp;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EntityResponseUtils {

  private EntityResponseUtils() {}

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
    final CorpUser emptyCreatedUser = new CorpUser();
    emptyCreatedUser.setUrn(aspect.getCreated().getActor().toString());
    auditStamp.setActor(emptyCreatedUser);
    auditStamp.setTime(aspect.getCreated().getTime());
    return auditStamp;
  }
}
