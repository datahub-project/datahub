package com.linkedin.datahub.graphql.types.rolemetadata.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.Actor;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Role;
import com.linkedin.datahub.graphql.generated.RoleProperties;
import com.linkedin.datahub.graphql.generated.RoleUser;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.RoleKey;
import com.linkedin.role.Actors;
import com.linkedin.role.RoleUserArray;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class RoleMapper implements ModelMapper<EntityResponse, Role> {

  public static final RoleMapper INSTANCE = new RoleMapper();

  public static Role map(@Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(entityResponse);
  }

  private static RoleProperties mapRoleProperties(final com.linkedin.role.RoleProperties e) {
    final RoleProperties propertiesResult = new RoleProperties();
    propertiesResult.setName(e.getName());
    propertiesResult.setDescription(e.getDescription());
    propertiesResult.setType(e.getType());
    propertiesResult.setRequestUrl(e.getRequestUrl());

    return propertiesResult;
  }

  private static RoleUser mapCorpUsers(final com.linkedin.role.RoleUser provisionedUser) {
    RoleUser result = new RoleUser();
    CorpUser corpUser = new CorpUser();
    corpUser.setUrn(provisionedUser.getUser().toString());
    result.setUser(corpUser);
    return result;
  }

  private static Actor mapActor(Actors actors) {
    Actor actor = new Actor();
    actor.setUsers(mapRoleUsers(actors.getUsers()));
    return actor;
  }

  private static List<RoleUser> mapRoleUsers(RoleUserArray users) {
    if (users == null) {
      return null;
    }
    return users.stream().map(x -> mapCorpUsers(x)).collect(Collectors.toList());
  }

  @Override
  public Role apply(EntityResponse input) {

    final Role result = new Role();
    final Urn entityUrn = input.getUrn();

    result.setUrn(entityUrn.toString());
    result.setType(EntityType.ROLE);

    final EnvelopedAspectMap aspects = input.getAspects();

    final EnvelopedAspect roleKeyAspect = aspects.get(Constants.ROLE_KEY);
    if (roleKeyAspect != null) {
      result.setId(new RoleKey(roleKeyAspect.getValue().data()).getId());
    }
    final EnvelopedAspect envelopedPropertiesAspect =
        aspects.get(Constants.ROLE_PROPERTIES_ASPECT_NAME);
    if (envelopedPropertiesAspect != null) {
      result.setProperties(
          mapRoleProperties(
              new com.linkedin.role.RoleProperties(envelopedPropertiesAspect.getValue().data())));
    }

    final EnvelopedAspect envelopedUsers = aspects.get(Constants.ROLE_ACTORS_ASPECT_NAME);
    if (envelopedUsers != null) {
      result.setActors(mapActor(new Actors(envelopedUsers.getValue().data())));
    }

    return result;
  }
}
