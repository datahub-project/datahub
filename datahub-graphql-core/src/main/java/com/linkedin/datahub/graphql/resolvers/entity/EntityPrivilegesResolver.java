package com.linkedin.datahub.graphql.resolvers.entity;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityPrivileges;
import com.linkedin.datahub.graphql.resolvers.mutate.util.EmbedUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

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

    return CompletableFuture.supplyAsync(
        () -> {
          switch (urn.getEntityType()) {
            case Constants.GLOSSARY_TERM_ENTITY_NAME:
              return getGlossaryTermPrivileges(urn, context);
            case Constants.GLOSSARY_NODE_ENTITY_NAME:
              return getGlossaryNodePrivileges(urn, context);
            case Constants.DATASET_ENTITY_NAME:
              return getDatasetPrivileges(urn, context);
            case Constants.CHART_ENTITY_NAME:
              return getChartPrivileges(urn, context);
            case Constants.DASHBOARD_ENTITY_NAME:
              return getDashboardPrivileges(urn, context);
            case Constants.DATA_JOB_ENTITY_NAME:
              return getDataJobPrivileges(urn, context);
            default:
              log.warn(
                  "Tried to get entity privileges for entity type {} but nothing is implemented for it yet",
                  urn.getEntityType());
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
      Boolean canManage =
          GlossaryUtils.canManageChildrenEntities(context, parentNodeUrn, _entityClient);
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
    Boolean canManageChildren =
        GlossaryUtils.canManageChildrenEntities(context, nodeUrn, _entityClient);
    result.setCanManageChildren(canManageChildren);

    Urn parentNodeUrn = GlossaryUtils.getParentUrn(nodeUrn, context, _entityClient);
    if (parentNodeUrn != null) {
      Boolean canManage =
          GlossaryUtils.canManageChildrenEntities(context, parentNodeUrn, _entityClient);
      result.setCanManageEntity(canManage);
    }
    return result;
  }

  private boolean canEditEntityLineage(Urn urn, QueryContext context) {
    final ConjunctivePrivilegeGroup allPrivilegesGroup =
        new ConjunctivePrivilegeGroup(
            ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()));
    DisjunctivePrivilegeGroup orPrivilegesGroup =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                allPrivilegesGroup,
                new ConjunctivePrivilegeGroup(
                    Collections.singletonList(PoliciesConfig.EDIT_LINEAGE_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        urn.getEntityType(),
        urn.toString(),
        orPrivilegesGroup);
  }

  private EntityPrivileges getDatasetPrivileges(Urn urn, QueryContext context) {
    final EntityPrivileges result = new EntityPrivileges();
    result.setCanEditLineage(canEditEntityLineage(urn, context));
    result.setCanEditEmbed(EmbedUtils.isAuthorizedToUpdateEmbedForEntity(urn, context));
    result.setCanEditQueries(AuthorizationUtils.canCreateQuery(ImmutableList.of(urn), context));
    return result;
  }

  private EntityPrivileges getChartPrivileges(Urn urn, QueryContext context) {
    final EntityPrivileges result = new EntityPrivileges();
    result.setCanEditLineage(canEditEntityLineage(urn, context));
    result.setCanEditEmbed(EmbedUtils.isAuthorizedToUpdateEmbedForEntity(urn, context));
    return result;
  }

  private EntityPrivileges getDashboardPrivileges(Urn urn, QueryContext context) {
    final EntityPrivileges result = new EntityPrivileges();
    result.setCanEditLineage(canEditEntityLineage(urn, context));
    result.setCanEditEmbed(EmbedUtils.isAuthorizedToUpdateEmbedForEntity(urn, context));
    return result;
  }

  private EntityPrivileges getDataJobPrivileges(Urn urn, QueryContext context) {
    final EntityPrivileges result = new EntityPrivileges();
    result.setCanEditLineage(canEditEntityLineage(urn, context));
    return result;
  }
}
