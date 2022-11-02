package com.linkedin.datahub.graphql.resolvers.entity;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityPrivileges;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
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
    if (GlossaryUtils.canManageGlossaries(context)) {
      result.setCanManageEntity(true);
      return result;
    }
    Urn parentNodeUrn = GlossaryUtils.getTermParentUrn(termUrn, context, _entityClient);
    if (parentNodeUrn != null) {
      Boolean canManage = GlossaryUtils.canManageGlossaryEntity(context, parentNodeUrn);
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
    Boolean canManageChildren = GlossaryUtils.canManageGlossaryEntity(context, nodeUrn);
    result.setCanManageChildren(canManageChildren);

    Urn parentNodeUrn = GlossaryUtils.getNodeParentUrn(nodeUrn, context, _entityClient);
    if (parentNodeUrn != null) {
      Boolean canManage = GlossaryUtils.canManageGlossaryEntity(context, parentNodeUrn);
      result.setCanManageEntity(canManage);
    }
    return result;
  }
}
