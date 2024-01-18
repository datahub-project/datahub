package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.persistAspect;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.UpdateNameInput;
import com.linkedin.datahub.graphql.resolvers.dataproduct.DataProductAuthorizationUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.DomainUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
import com.linkedin.dataproduct.DataProductProperties;
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

  private final EntityService _entityService;
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final UpdateNameInput input =
        bindArgument(environment.getArgument("input"), UpdateNameInput.class);
    Urn targetUrn = Urn.createFromString(input.getUrn());
    log.info("Updating name. input: {}", input);

    return CompletableFuture.supplyAsync(
        () -> {
          if (!_entityService.exists(targetUrn)) {
            throw new IllegalArgumentException(
                String.format("Failed to update %s. %s does not exist.", targetUrn, targetUrn));
          }

          switch (targetUrn.getEntityType()) {
            case Constants.GLOSSARY_TERM_ENTITY_NAME:
              return updateGlossaryTermName(targetUrn, input, environment.getContext());
            case Constants.GLOSSARY_NODE_ENTITY_NAME:
              return updateGlossaryNodeName(targetUrn, input, environment.getContext());
            case Constants.DOMAIN_ENTITY_NAME:
              return updateDomainName(targetUrn, input, environment.getContext());
            case Constants.CORP_GROUP_ENTITY_NAME:
              return updateGroupName(targetUrn, input, environment.getContext());
            case Constants.DATA_PRODUCT_ENTITY_NAME:
              return updateDataProductName(targetUrn, input, environment.getContext());
            default:
              throw new RuntimeException(
                  String.format(
                      "Failed to update name. Unsupported resource type %s provided.", targetUrn));
          }
        });
  }

  private Boolean updateGlossaryTermName(
      Urn targetUrn, UpdateNameInput input, QueryContext context) {
    final Urn parentNodeUrn = GlossaryUtils.getParentUrn(targetUrn, context, _entityClient);
    if (GlossaryUtils.canManageChildrenEntities(context, parentNodeUrn, _entityClient)) {
      try {
        GlossaryTermInfo glossaryTermInfo =
            (GlossaryTermInfo)
                EntityUtils.getAspectFromEntity(
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
    final Urn parentNodeUrn = GlossaryUtils.getParentUrn(targetUrn, context, _entityClient);
    if (GlossaryUtils.canManageChildrenEntities(context, parentNodeUrn, _entityClient)) {
      try {
        GlossaryNodeInfo glossaryNodeInfo =
            (GlossaryNodeInfo)
                EntityUtils.getAspectFromEntity(
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
            targetUrn, Constants.CORP_GROUP_INFO_ASPECT_NAME, corpGroupInfo, actor, _entityService);

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
                  targetUrn.toString(), Constants.DOMAINS_ASPECT_NAME, _entityService, null);
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
}
