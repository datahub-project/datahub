package com.linkedin.datahub.graphql.resolvers.entity;

import static com.linkedin.metadata.authorization.ApiGroup.LINEAGE;
import static com.linkedin.metadata.authorization.ApiOperation.UPDATE;

import com.datahub.authorization.AuthUtil;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityPrivileges;
import com.linkedin.datahub.graphql.resolvers.mutate.util.EmbedUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
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

    return GraphQLConcurrencyUtils.supplyAsync(
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
                  "Tried to get entity privileges for entity type {}. Adding common privileges only.",
                  urn.getEntityType());
              EntityPrivileges commonPrivileges = new EntityPrivileges();
              addCommonPrivileges(commonPrivileges, urn, context);
              return commonPrivileges;
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private EntityPrivileges getGlossaryTermPrivileges(Urn termUrn, QueryContext context) {
    final EntityPrivileges result = new EntityPrivileges();
    addCommonPrivileges(result, termUrn, context);
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
    addCommonPrivileges(result, nodeUrn, context);
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
    return AuthUtil.isAuthorizedUrns(context.getOperationContext(), LINEAGE, UPDATE, List.of(urn));
  }

  private EntityPrivileges getDatasetPrivileges(Urn urn, QueryContext context) {
    final EntityPrivileges result = new EntityPrivileges();
    result.setCanEditEmbed(EmbedUtils.isAuthorizedToUpdateEmbedForEntity(urn, context));
    result.setCanEditQueries(AuthorizationUtils.canCreateQuery(ImmutableList.of(urn), context));
    addCommonPrivileges(result, urn, context);
    return result;
  }

  private EntityPrivileges getChartPrivileges(Urn urn, QueryContext context) {
    final EntityPrivileges result = new EntityPrivileges();
    result.setCanEditEmbed(EmbedUtils.isAuthorizedToUpdateEmbedForEntity(urn, context));
    addCommonPrivileges(result, urn, context);
    return result;
  }

  private EntityPrivileges getDashboardPrivileges(Urn urn, QueryContext context) {
    final EntityPrivileges result = new EntityPrivileges();
    result.setCanEditEmbed(EmbedUtils.isAuthorizedToUpdateEmbedForEntity(urn, context));
    addCommonPrivileges(result, urn, context);
    return result;
  }

  private EntityPrivileges getDataJobPrivileges(Urn urn, QueryContext context) {
    final EntityPrivileges result = new EntityPrivileges();
    addCommonPrivileges(result, urn, context);
    return result;
  }

  private void addCommonPrivileges(
      @Nonnull EntityPrivileges result, @Nonnull Urn urn, @Nonnull QueryContext context) {
    result.setCanEditLineage(canEditEntityLineage(urn, context));
    result.setCanEditProperties(AuthorizationUtils.canEditProperties(urn, context));
  }
}
