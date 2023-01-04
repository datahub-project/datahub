package com.linkedin.datahub.graphql.resolvers.entity;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityPrivileges;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
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
        default:
          log.warn("Tried to get entity privileges for entity type {} but nothing is implemented for it yet", urn.getEntityType());
          return new EntityPrivileges();
      }
    });
  }

  private EntityPrivileges getGlossaryTermPrivileges(Urn termUrn, QueryContext context) {
    final EntityPrivileges result = new EntityPrivileges();
    result.setCanManageEntity(false);
    if (GlossaryUtils.canManageGlossaries(context)) {
      result.setCanManageEntity(true);
      return result;
    }
    Urn parentNodeUrn = GlossaryUtils.getParentUrn(termUrn, context, _entityClient);
    if (parentNodeUrn != null) {
      Boolean canManage = GlossaryUtils.canManageChildrenEntities(context, parentNodeUrn);
      result.setCanManageEntity(canManage);
    }
    return result;
  }

  private EntityPrivileges getGlossaryNodePrivileges(Urn nodeUrn, QueryContext context) {
    final EntityPrivileges result = new EntityPrivileges();
    result.setCanManageEntity(false);
    if (GlossaryUtils.canManageGlossaries(context)) {
      result.setCanManageEntity(true);
      result.setCanManageChildren(true);
      return result;
    }
    Boolean canManageChildren = GlossaryUtils.canManageChildrenEntities(context, nodeUrn);
    result.setCanManageChildren(canManageChildren);

    Urn parentNodeUrn = GlossaryUtils.getParentUrn(nodeUrn, context, _entityClient);
    if (parentNodeUrn != null) {
      Boolean canManage = GlossaryUtils.canManageChildrenEntities(context, parentNodeUrn);
      result.setCanManageEntity(canManage);
    }
    return result;
  }
}
