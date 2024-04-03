package com.datahub.authorization.fieldresolverprovider;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Provides field resolver for owners given entitySpec */
@Slf4j
@RequiredArgsConstructor
public class TagFieldResolverProvider implements EntityFieldResolverProvider {

  private final EntityClient _entityClient;
  private final Authentication _systemAuthentication;

  @Override
  public List<EntityFieldType> getFieldTypes() {
    return Collections.singletonList(EntityFieldType.TAG);
  }

  @Override
  public FieldResolver getFieldResolver(EntitySpec entitySpec) {
    return FieldResolver.getResolverFromFunction(entitySpec, this::getTags);
  }

  private FieldResolver.FieldValue getTags(EntitySpec entitySpec) {
    Urn entityUrn = UrnUtils.getUrn(entitySpec.getEntity());
    EnvelopedAspect globalTagsAspect;
    try {
      EntityResponse response =
          _entityClient.getV2(
              entityUrn.getEntityType(),
              entityUrn,
              Collections.singleton(Constants.GLOBAL_TAGS_ASPECT_NAME),
              _systemAuthentication);
      if (response == null
          || !response.getAspects().containsKey(Constants.GLOBAL_TAGS_ASPECT_NAME)) {
        return FieldResolver.emptyFieldValue();
      }
      globalTagsAspect = response.getAspects().get(Constants.GLOBAL_TAGS_ASPECT_NAME);
    } catch (Exception e) {
      log.error("Error while retrieving tags aspect for urn {}", entityUrn, e);
      return FieldResolver.emptyFieldValue();
    }
    GlobalTags globalTags = new GlobalTags(globalTagsAspect.getValue().data());
    return FieldResolver.FieldValue.builder()
        .values(
            globalTags.getTags().stream()
                .map(tag -> tag.getTag().toString())
                .collect(Collectors.toSet()))
        .build();
  }
}
