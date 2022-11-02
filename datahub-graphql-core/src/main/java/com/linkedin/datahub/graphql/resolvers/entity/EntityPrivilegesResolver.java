package com.linkedin.datahub.graphql.resolvers.entity;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityPrivileges;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;

public class EntityPrivilegesResolver implements DataFetcher<CompletableFuture<EntityPrivileges>> {

  private final EntityClient _entityClient;

  public EntityPrivilegesResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<EntityPrivileges> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final String urnString = ((Entity) environment.getSource()).getUrn();
    final Urn urn = UrnUtils.getUrn(urnString);

    return CompletableFuture.supplyAsync(() -> {
      switch (urn.getEntityType()) {
        case Constants.GLOSSARY_TERM_ENTITY_NAME:
          return getGlossaryTermPrivileges(urn, context);
        case Constants.GLOSSARY_NODE_ENTITY_NAME:
          return getGlossaryNodePrivileges(urn, context);
      }
      return new EntityPrivileges();
    });
  }

  private EntityPrivileges getGlossaryTermPrivileges(Urn termUrn, QueryContext context) {
    final EntityPrivileges result = new EntityPrivileges();
    result.setCanManageEntity(false);
    if (AuthorizationUtils.canManageGlossaries(context)) {
      result.setCanManageEntity(true);
      return result;
    }
    try {
      EntityResponse response = _entityClient.getV2(Constants.GLOSSARY_TERM_ENTITY_NAME, termUrn,
          ImmutableSet.of(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME), context.getAuthentication());
      if (response.getAspects().get(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME) != null) {
        GlossaryTermInfo termInfo = new GlossaryTermInfo(response.getAspects()
            .get(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME).getValue().data());
        if (termInfo.hasParentNode()) {
          Boolean canManage = AuthorizationUtils.canManageGlossaryEntity(context, termInfo.getParentNode());
          result.setCanManageEntity(canManage);
        }
      }
      return result;
    } catch (URISyntaxException | RemoteInvocationException e) {
      throw new RuntimeException("Failed to fetch Glossary Term to check for privileges", e);
    }
  }

  private EntityPrivileges getGlossaryNodePrivileges(Urn nodeUrn, QueryContext context) {
    final EntityPrivileges result = new EntityPrivileges();
    result.setCanManageEntity(false);
    if (AuthorizationUtils.canManageGlossaries(context)) {
      result.setCanManageEntity(true);
      result.setCanManageChildren(true);
      return result;
    }
    Boolean canManageChildren = AuthorizationUtils.canManageGlossaryEntity(context, nodeUrn);
    result.setCanManageChildren(canManageChildren);
    // get parent node to see if you can manage this entity
    try {
      EntityResponse response = _entityClient.getV2(Constants.GLOSSARY_NODE_ENTITY_NAME, nodeUrn,
          ImmutableSet.of(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME), context.getAuthentication());
      if (response.getAspects().get(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME) != null) {
        GlossaryNodeInfo nodeInfo = new GlossaryNodeInfo(response.getAspects()
            .get(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME).getValue().data());
        if (nodeInfo.hasParentNode()) {
          Boolean canManage = AuthorizationUtils.canManageGlossaryEntity(context, nodeInfo.getParentNode());
          result.setCanManageEntity(canManage);
        }
      }
      return result;
    } catch (URISyntaxException | RemoteInvocationException e) {
      throw new RuntimeException("Failed to fetch Glossary Node to check for privileges", e);
    }
  }
}
