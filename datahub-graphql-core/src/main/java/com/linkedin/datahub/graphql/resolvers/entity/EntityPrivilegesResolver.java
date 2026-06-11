package com.linkedin.datahub.graphql.resolvers.entity;

import static com.linkedin.datahub.graphql.resolvers.entity.EntityPrivilegesFields.*;
import static com.linkedin.metadata.Constants.WILDCARD_URN;
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
import com.linkedin.datahub.graphql.resolvers.assertion.AssertionUtils;
import com.linkedin.datahub.graphql.resolvers.businessattribute.BusinessAttributeAuthorizationUtils;
import com.linkedin.datahub.graphql.resolvers.dataproduct.DataProductAuthorizationUtils;
import com.linkedin.datahub.graphql.resolvers.incident.IncidentUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.DescriptionUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.DeprecationUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.DomainUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.EmbedUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.LabelUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.LinkUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.OwnerUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.SelectedField;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Collectors;
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

    // Each EntityPrivileges field is the result of an independent authorization check. The
    // resolver only computes query selected fields, to avoid unnecessary auth checks and improve
    // performance.
    final Set<String> selected =
        environment.getSelectionSet().getImmediateFields().stream()
            .map(SelectedField::getName)
            .collect(Collectors.toSet());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          switch (urn.getEntityType()) {
            case Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME:
              return getBusinessAttributePrivileges(urn, context, selected);
            case Constants.GLOSSARY_TERM_ENTITY_NAME:
              return getGlossaryTermPrivileges(urn, context, selected);
            case Constants.GLOSSARY_NODE_ENTITY_NAME:
              return getGlossaryNodePrivileges(urn, context, selected);
            case Constants.DATASET_ENTITY_NAME:
              return getDatasetPrivileges(urn, context, selected);
            case Constants.CHART_ENTITY_NAME:
              return getChartPrivileges(urn, context, selected);
            case Constants.DASHBOARD_ENTITY_NAME:
              return getDashboardPrivileges(urn, context, selected);
            case Constants.DATA_JOB_ENTITY_NAME:
              return getDataJobPrivileges(urn, context, selected);
            case Constants.DOCUMENT_ENTITY_NAME:
              return getDocumentPrivileges(urn, context, selected);
            default:
              log.warn(
                  "Tried to get entity privileges for entity type {}. Adding common privileges only.",
                  urn.getEntityType());
              EntityPrivileges commonPrivileges = new EntityPrivileges();
              addCommonPrivileges(commonPrivileges, urn, context, selected);
              return commonPrivileges;
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Runs an authorization check and stores its result only when the query selected {@code
   * fieldName}. The field name must match the {@code EntityPrivileges} field in {@code
   * auth.graphql}; a mismatch silently leaves the field null (guarded by resolver tests).
   */
  private static void computeIfSelected(
      @Nonnull Set<String> selected,
      @Nonnull String fieldName,
      @Nonnull BooleanSupplier check,
      @Nonnull Consumer<Boolean> setter) {
    if (selected.contains(fieldName)) {
      setter.accept(check.getAsBoolean());
    }
  }

  private EntityPrivileges getBusinessAttributePrivileges(
      Urn urn, QueryContext context, Set<String> selected) {
    final EntityPrivileges result = new EntityPrivileges();
    computeIfSelected(
        selected,
        CAN_MANAGE_ENTITY,
        () -> BusinessAttributeAuthorizationUtils.canManageBusinessAttribute(context),
        result::setCanManageEntity);
    addCommonPrivileges(result, urn, context, selected);
    return result;
  }

  private EntityPrivileges getGlossaryTermPrivileges(
      Urn termUrn, QueryContext context, Set<String> selected) {
    final EntityPrivileges result = new EntityPrivileges();
    addCommonPrivileges(result, termUrn, context, selected);
    if (selected.contains(CAN_MANAGE_ENTITY)) {
      result.setCanManageEntity(computeTermCanManageEntity(termUrn, context));
    }
    return result;
  }

  private boolean computeTermCanManageEntity(Urn termUrn, QueryContext context) {
    if (GlossaryUtils.canManageGlossaries(context)) {
      return true;
    }
    Urn parentNodeUrn = GlossaryUtils.getParentUrn(termUrn, context, _entityClient);
    return parentNodeUrn != null
        && GlossaryUtils.canManageChildrenEntities(context, parentNodeUrn, _entityClient);
  }

  private EntityPrivileges getGlossaryNodePrivileges(
      Urn nodeUrn, QueryContext context, Set<String> selected) {
    final EntityPrivileges result = new EntityPrivileges();
    addCommonPrivileges(result, nodeUrn, context, selected);

    final boolean needChildren = selected.contains(CAN_MANAGE_CHILDREN);
    final boolean needEntity = selected.contains(CAN_MANAGE_ENTITY);
    if (!needChildren && !needEntity) {
      return result;
    }

    // canManageGlossaries grants both manage privileges; check it once before the parent walk.
    if (GlossaryUtils.canManageGlossaries(context)) {
      if (needChildren) {
        result.setCanManageChildren(true);
      }
      if (needEntity) {
        result.setCanManageEntity(true);
      }
      return result;
    }

    if (needChildren) {
      result.setCanManageChildren(
          GlossaryUtils.canManageChildrenEntities(context, nodeUrn, _entityClient));
    }
    if (needEntity) {
      Urn parentNodeUrn = GlossaryUtils.getParentUrn(nodeUrn, context, _entityClient);
      result.setCanManageEntity(
          parentNodeUrn != null
              && GlossaryUtils.canManageChildrenEntities(context, parentNodeUrn, _entityClient));
    }
    return result;
  }

  private boolean canEditEntityLineage(Urn urn, QueryContext context) {
    return AuthUtil.isAuthorizedUrns(context.getOperationContext(), LINEAGE, UPDATE, List.of(urn));
  }

  private EntityPrivileges getDatasetPrivileges(
      Urn urn, QueryContext context, Set<String> selected) {
    final EntityPrivileges result = new EntityPrivileges();
    computeIfSelected(
        selected,
        CAN_EDIT_EMBED,
        () -> EmbedUtils.isAuthorizedToUpdateEmbedForEntity(urn, context),
        result::setCanEditEmbed);
    computeIfSelected(
        selected,
        CAN_EDIT_QUERIES,
        () -> AuthorizationUtils.canCreateQuery(ImmutableList.of(urn), context),
        result::setCanEditQueries);
    // Schema Field Edits are a bit of a hack.
    computeIfSelected(
        selected,
        CAN_EDIT_SCHEMA_FIELD_TAGS,
        () ->
            LabelUtils.isAuthorizedToUpdateTags(
                context, urn, "ignored", Collections.singleton(WILDCARD_URN)),
        result::setCanEditSchemaFieldTags);
    computeIfSelected(
        selected,
        CAN_EDIT_SCHEMA_FIELD_GLOSSARY_TERMS,
        () -> LabelUtils.isAuthorizedToUpdateTerms(context, urn, "ignored"),
        result::setCanEditSchemaFieldGlossaryTerms);
    computeIfSelected(
        selected,
        CAN_EDIT_SCHEMA_FIELD_DESCRIPTION,
        () -> DescriptionUtils.isAuthorizedToUpdateFieldDescription(context, urn),
        result::setCanEditSchemaFieldDescription);
    computeIfSelected(
        selected,
        CAN_VIEW_DATASET_USAGE,
        () -> AuthorizationUtils.isViewDatasetUsageAuthorized(context, urn),
        result::setCanViewDatasetUsage);
    computeIfSelected(
        selected,
        CAN_VIEW_DATASET_PROFILE,
        () -> AuthorizationUtils.isViewDatasetProfileAuthorized(context, urn),
        result::setCanViewDatasetProfile);
    computeIfSelected(
        selected,
        CAN_VIEW_DATASET_OPERATIONS,
        () -> AuthorizationUtils.isViewDatasetOperationsAuthorized(context, urn),
        result::setCanViewDatasetOperations);
    addCommonPrivileges(result, urn, context, selected);
    return result;
  }

  private EntityPrivileges getChartPrivileges(Urn urn, QueryContext context, Set<String> selected) {
    final EntityPrivileges result = new EntityPrivileges();
    computeIfSelected(
        selected,
        CAN_EDIT_EMBED,
        () -> EmbedUtils.isAuthorizedToUpdateEmbedForEntity(urn, context),
        result::setCanEditEmbed);
    addCommonPrivileges(result, urn, context, selected);
    return result;
  }

  private EntityPrivileges getDashboardPrivileges(
      Urn urn, QueryContext context, Set<String> selected) {
    final EntityPrivileges result = new EntityPrivileges();
    computeIfSelected(
        selected,
        CAN_EDIT_EMBED,
        () -> EmbedUtils.isAuthorizedToUpdateEmbedForEntity(urn, context),
        result::setCanEditEmbed);
    addCommonPrivileges(result, urn, context, selected);
    return result;
  }

  private EntityPrivileges getDataJobPrivileges(
      Urn urn, QueryContext context, Set<String> selected) {
    final EntityPrivileges result = new EntityPrivileges();
    addCommonPrivileges(result, urn, context, selected);
    return result;
  }

  private EntityPrivileges getDocumentPrivileges(
      Urn urn, QueryContext context, Set<String> selected) {
    final EntityPrivileges result = new EntityPrivileges();
    addCommonPrivileges(result, urn, context, selected);
    // Document-specific: canManageEntity includes ability to delete/move documents
    computeIfSelected(
        selected,
        CAN_MANAGE_ENTITY,
        () -> AuthorizationUtils.canEditDocument(urn, context),
        result::setCanManageEntity);
    return result;
  }

  private void addCommonPrivileges(
      @Nonnull EntityPrivileges result,
      @Nonnull Urn urn,
      @Nonnull QueryContext context,
      @Nonnull Set<String> selected) {
    computeIfSelected(
        selected,
        CAN_EDIT_LINEAGE,
        () -> canEditEntityLineage(urn, context),
        result::setCanEditLineage);
    computeIfSelected(
        selected,
        CAN_EDIT_PROPERTIES,
        () -> AuthorizationUtils.canEditProperties(urn, context),
        result::setCanEditProperties);
    computeIfSelected(
        selected,
        CAN_EDIT_ASSERTIONS,
        () -> AssertionUtils.isAuthorizedToEditAssertionFromAssertee(context, urn),
        result::setCanEditAssertions);
    computeIfSelected(
        selected,
        CAN_EDIT_ASSERTION_OWNERS,
        () -> OwnerUtils.isAuthorizedToUpdateAssertionOwnersOnEntity(context, urn),
        result::setCanEditAssertionOwners);
    computeIfSelected(
        selected,
        CAN_EDIT_INCIDENTS,
        () -> IncidentUtils.isAuthorizedToEditIncidentForResource(urn, context),
        result::setCanEditIncidents);
    computeIfSelected(
        selected,
        CAN_EDIT_DOMAINS,
        () -> DomainUtils.isAuthorizedToUpdateDomainsForEntity(context, urn, _entityClient),
        result::setCanEditDomains);
    computeIfSelected(
        selected,
        CAN_EDIT_DATA_PRODUCTS,
        () -> DataProductAuthorizationUtils.isAuthorizedToUpdateDataProductsForEntity(context, urn),
        result::setCanEditDataProducts);
    computeIfSelected(
        selected,
        CAN_EDIT_DEPRECATION,
        () -> DeprecationUtils.isAuthorizedToUpdateDeprecationForEntity(context, urn),
        result::setCanEditDeprecation);
    computeIfSelected(
        selected,
        CAN_EDIT_GLOSSARY_TERMS,
        () -> LabelUtils.isAuthorizedToUpdateTerms(context, urn, null),
        result::setCanEditGlossaryTerms);
    computeIfSelected(
        selected,
        CAN_EDIT_TAGS,
        () ->
            LabelUtils.isAuthorizedToUpdateTags(
                context, urn, null, Collections.singleton(WILDCARD_URN)),
        result::setCanEditTags);
    computeIfSelected(
        selected,
        CAN_EDIT_OWNERS,
        () -> OwnerUtils.isAuthorizedToUpdateOwners(context, urn),
        result::setCanEditOwners);
    computeIfSelected(
        selected,
        CAN_EDIT_DESCRIPTION,
        () -> DescriptionUtils.isAuthorizedToUpdateDescription(context, urn),
        result::setCanEditDescription);
    computeIfSelected(
        selected,
        CAN_EDIT_LINKS,
        () -> LinkUtils.isAuthorizedToUpdateLinks(context, urn),
        result::setCanEditLinks);
    computeIfSelected(
        selected,
        CAN_MANAGE_ASSET_SUMMARY,
        () -> AuthorizationUtils.canManageAssetSummary(context, urn),
        result::setCanManageAssetSummary);
  }
}
