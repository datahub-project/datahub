package com.linkedin.datahub.graphql.resolvers.lifecycle;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.lifecycle.LifecycleStageSettings;
import com.linkedin.lifecycle.LifecycleStageTypeInfo;

/** Shared test helpers for lifecycle resolver tests. */
final class LifecycleStageTestUtils {

  private LifecycleStageTestUtils() {}

  static EntityResponse makeStageResponse(Urn urn, String name, boolean hideInSearch) {
    LifecycleStageSettings settings = new LifecycleStageSettings();
    settings.setHideInSearch(hideInSearch);

    AuditStamp stamp = new AuditStamp();
    stamp.setTime(0L);
    stamp.setActor(UrnUtils.getUrn("urn:li:corpuser:system"));

    LifecycleStageTypeInfo info = new LifecycleStageTypeInfo();
    info.setName(name);
    info.setSettings(settings);
    info.setCreated(stamp);
    info.setLastModified(stamp);

    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(info.data()));

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(LifecycleStageTypeMapper.INFO_ASPECT, envelopedAspect);

    EntityResponse response = new EntityResponse();
    response.setUrn(urn);
    response.setEntityName(LifecycleStageTypeMapper.ENTITY_NAME);
    response.setAspects(aspectMap);
    return response;
  }
}
