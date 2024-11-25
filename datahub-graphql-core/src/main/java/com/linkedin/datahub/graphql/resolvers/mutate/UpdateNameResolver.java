package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.persistAspect;

import com.linkedin.businessattribute.BusinessAttributeInfo;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.UpdateNameInput;
import com.linkedin.datahub.graphql.resolvers.businessattribute.BusinessAttributeAuthorizationUtils;
import com.linkedin.datahub.graphql.resolvers.dataproduct.DataProductAuthorizationUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.BusinessAttributeUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.DomainUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.domain.DomainProperties;
import com.linkedin.domain.Domains;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class UpdateNameResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService<?> _entityService;
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final UpdateNameInput input =
        bindArgument(environment.getArgument("input"), UpdateNameInput.class);
    Urn targetUrn = Urn.createFromString(input.getUrn());
    log.info("Updating name. input: {}", input);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!_entityService.exists(context.getOperationContext(), targetUrn, true)) {
            throw new IllegalArgumentException(
                String.format("Failed to update %s. %s does not exist.", targetUrn, targetUrn));
          }

          switch (targetUrn.getEntityType()) {
            case Constants.GLOSSARY_TERM_ENTITY_NAME:
              return updateGlossaryTermName(targetUrn, input, context);
            case Constants.GLOSSARY_NODE_ENTITY_NAME:
              return updateGlossaryNodeName(targetUrn, input, context);
            case Constants.DOMAIN_ENTITY_NAME:
              return updateDomainName(targetUrn, input, context);
            case Constants.CORP_GROUP_ENTITY_NAME:
              return updateGroupName(targetUrn, input, context);
            case Constants.DATA_PRODUCT_ENTITY_NAME:
              return updateDataProductName(targetUrn, input, context);
            case Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME:
              return updateBusinessAttributeName(targetUrn, input, environment.getContext());
            case Constants.DATASET_ENTITY_NAME:
              return updateDatasetName(targetUrn, input, environment.getContext());
            default:
              throw new RuntimeException(
                  String.format(
                      "Failed to update name. Unsupported resource type %s provided.", targetUrn));
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private Boolean updateGlossaryTermName(
      Urn targetUrn, UpdateNameInput input, QueryContext context) {
    if (GlossaryUtils.canUpdateGlossaryEntity(targetUrn, context, _entityClient)) {
      try {
        GlossaryTermInfo glossaryTermInfo =
            (GlossaryTermInfo)
                EntityUtils.getAspectFromEntity(
                    context.getOperationContext(),
                    targetUrn.toString(),
                    Constants.GLOSSARY_TERM_INFO_ASPECT_NAME,
                    _entityService,
                    null);
        if (glossaryTermInfo == null) {
          throw new IllegalArgumentException("Glossary Term does not exist");
        }
        glossaryTermInfo.setName(input.getName());
        Urn actor = UrnUtils.getUrn(context.getActorUrn());
        persistAspect(
            context.getOperationContext(),
            targetUrn,
            Constants.GLOSSARY_TERM_INFO_ASPECT_NAME,
            glossaryTermInfo,
            actor,
            _entityService);

        return true;
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to perform update against input %s", input), e);
      }
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  private Boolean updateGlossaryNodeName(
      Urn targetUrn, UpdateNameInput input, QueryContext context) {
    if (GlossaryUtils.canUpdateGlossaryEntity(targetUrn, context, _entityClient)) {
      try {
        GlossaryNodeInfo glossaryNodeInfo =
            (GlossaryNodeInfo)
                EntityUtils.getAspectFromEntity(
                    context.getOperationContext(),
                    targetUrn.toString(),
                    Constants.GLOSSARY_NODE_INFO_ASPECT_NAME,
                    _entityService,
                    null);
        if (glossaryNodeInfo == null) {
          throw new IllegalArgumentException("Glossary Node does not exist");
        }
        glossaryNodeInfo.setName(input.getName());
        Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
        persistAspect(
            context.getOperationContext(),
            targetUrn,
            Constants.GLOSSARY_NODE_INFO_ASPECT_NAME,
            glossaryNodeInfo,
            actor,
            _entityService);

        return true;
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to perform update against input %s", input), e);
      }
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  private Boolean updateDomainName(Urn targetUrn, UpdateNameInput input, QueryContext context) {
    if (AuthorizationUtils.canManageDomains(context)) {
      try {
        DomainProperties domainProperties =
            (DomainProperties)
                EntityUtils.getAspectFromEntity(
                    context.getOperationContext(),
                    targetUrn.toString(),
                    Constants.DOMAIN_PROPERTIES_ASPECT_NAME,
                    _entityService,
                    null);

        if (domainProperties == null) {
          throw new IllegalArgumentException("Domain does not exist");
        }

        if (DomainUtils.hasNameConflict(
            input.getName(),
            DomainUtils.getParentDomainSafely(domainProperties),
            context,
            _entityClient)) {
          throw new DataHubGraphQLException(
              String.format(
                  "\"%s\" already exists in this domain. Please pick a unique name.",
                  input.getName()),
              DataHubGraphQLErrorCode.CONFLICT);
        }

        domainProperties.setName(input.getName());
        Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
        persistAspect(
            context.getOperationContext(),
            targetUrn,
            Constants.DOMAIN_PROPERTIES_ASPECT_NAME,
            domainProperties,
            actor,
            _entityService);

        return true;
      } catch (DataHubGraphQLException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to perform update against input %s", input), e);
      }
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  private Boolean updateGroupName(Urn targetUrn, UpdateNameInput input, QueryContext context) {
    if (AuthorizationUtils.canManageUsersAndGroups(context)) {
      try {
        CorpGroupInfo corpGroupInfo =
            (CorpGroupInfo)
                EntityUtils.getAspectFromEntity(
                    context.getOperationContext(),
                    targetUrn.toString(),
                    Constants.CORP_GROUP_INFO_ASPECT_NAME,
                    _entityService,
                    null);
        if (corpGroupInfo == null) {
          throw new IllegalArgumentException("Group does not exist");
        }
        corpGroupInfo.setDisplayName(input.getName());
        Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
        persistAspect(
            context.getOperationContext(),
            targetUrn,
            Constants.CORP_GROUP_INFO_ASPECT_NAME,
            corpGroupInfo,
            actor,
            _entityService);

        return true;
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to perform update against input %s", input), e);
      }
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  // udpates editable dataset properties aspect's name field
  private Boolean updateDatasetName(Urn targetUrn, UpdateNameInput input, QueryContext context) {
    if (AuthorizationUtils.canEditProperties(targetUrn, context)) {
      try {
        if (input.getName() != null) {
          final EditableDatasetProperties editableDatasetProperties =
              new EditableDatasetProperties();
          editableDatasetProperties.setName(input.getName());
          final AuditStamp auditStamp = new AuditStamp();
          Urn actor = UrnUtils.getUrn(context.getActorUrn());
          auditStamp.setActor(actor, SetMode.IGNORE_NULL);
          auditStamp.setTime(System.currentTimeMillis());
          editableDatasetProperties.setLastModified(auditStamp);
          persistAspect(
              context.getOperationContext(),
              targetUrn,
              Constants.EDITABLE_DATASET_PROPERTIES_ASPECT_NAME,
              editableDatasetProperties,
              actor,
              _entityService);
        }
        return true;
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to perform update against input %s", input), e);
      }
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  private Boolean updateDataProductName(
      Urn targetUrn, UpdateNameInput input, QueryContext context) {
    try {
      DataProductProperties dataProductProperties =
          (DataProductProperties)
              EntityUtils.getAspectFromEntity(
                  context.getOperationContext(),
                  targetUrn.toString(),
                  Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME,
                  _entityService,
                  null);
      if (dataProductProperties == null) {
        throw new IllegalArgumentException("Data Product does not exist");
      }

      Domains dataProductDomains =
          (Domains)
              EntityUtils.getAspectFromEntity(
                  context.getOperationContext(),
                  targetUrn.toString(),
                  Constants.DOMAINS_ASPECT_NAME,
                  _entityService,
                  null);
      if (dataProductDomains != null
          && dataProductDomains.hasDomains()
          && dataProductDomains.getDomains().size() > 0) {
        // get first domain since we only allow one domain right now
        Urn domainUrn = UrnUtils.getUrn(dataProductDomains.getDomains().get(0).toString());
        // if they can't edit a data product from either the parent domain permission or from
        // permission on the data product itself, throw error
        if (!DataProductAuthorizationUtils.isAuthorizedToManageDataProducts(context, domainUrn)
            && !DataProductAuthorizationUtils.isAuthorizedToEditDataProduct(context, targetUrn)) {
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        }
      } else {
        // should not happen since data products need to have a domain
        if (!DataProductAuthorizationUtils.isAuthorizedToEditDataProduct(context, targetUrn)) {
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        }
      }

      dataProductProperties.setName(input.getName());
      Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
      persistAspect(
          context.getOperationContext(),
          targetUrn,
          Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME,
          dataProductProperties,
          actor,
          _entityService);

      return true;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to perform update against input %s", input), e);
    }
  }

  private Boolean updateBusinessAttributeName(
      Urn targetUrn, UpdateNameInput input, QueryContext context) {
    if (!BusinessAttributeAuthorizationUtils.canManageBusinessAttribute(context)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }
    try {
      BusinessAttributeInfo businessAttributeInfo =
          (BusinessAttributeInfo)
              EntityUtils.getAspectFromEntity(
                  context.getOperationContext(),
                  targetUrn.toString(),
                  Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME,
                  _entityService,
                  null);
      if (businessAttributeInfo == null) {
        throw new IllegalArgumentException("Business Attribute does not exist");
      }

      if (BusinessAttributeUtils.hasNameConflict(input.getName(), context, _entityClient)) {
        throw new DataHubGraphQLException(
            String.format(
                "\"%s\" already exists as Business Attribute. Please pick a unique name.",
                input.getName()),
            DataHubGraphQLErrorCode.CONFLICT);
      }

      businessAttributeInfo.setFieldPath(input.getName());
      businessAttributeInfo.setName(input.getName());
      Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
      persistAspect(
          context.getOperationContext(),
          targetUrn,
          Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME,
          businessAttributeInfo,
          actor,
          _entityService);
      return true;
    } catch (DataHubGraphQLException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to perform update against input %s", input), e);
    }
  }
}
