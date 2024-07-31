package com.linkedin.datahub.graphql.resolvers.businessattribute;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.buildMetadataChangeProposalWithKey;
import static com.linkedin.metadata.Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME;

import com.linkedin.businessattribute.BusinessAttributeInfo;
import com.linkedin.businessattribute.BusinessAttributeKey;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.BusinessAttribute;
import com.linkedin.datahub.graphql.generated.CreateBusinessAttributeInput;
import com.linkedin.datahub.graphql.generated.OwnerEntityType;
import com.linkedin.datahub.graphql.resolvers.mutate.util.BusinessAttributeUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.OwnerUtils;
import com.linkedin.datahub.graphql.types.businessattribute.mappers.BusinessAttributeMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.BusinessAttributeService;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class CreateBusinessAttributeResolver
    implements DataFetcher<CompletableFuture<BusinessAttribute>> {
  private final EntityClient _entityClient;
  private final EntityService<?> _entityService;
  private final BusinessAttributeService businessAttributeService;

  @Override
  public CompletableFuture<BusinessAttribute> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    CreateBusinessAttributeInput input =
        bindArgument(environment.getArgument("input"), CreateBusinessAttributeInput.class);
    if (!BusinessAttributeAuthorizationUtils.canCreateBusinessAttribute(context)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final BusinessAttributeKey businessAttributeKey = new BusinessAttributeKey();
            String id = input.getId() != null ? input.getId() : UUID.randomUUID().toString();
            businessAttributeKey.setId(id);

            if (_entityClient.exists(
                context.getOperationContext(),
                EntityKeyUtils.convertEntityKeyToUrn(
                    businessAttributeKey, BUSINESS_ATTRIBUTE_ENTITY_NAME))) {
              throw new IllegalArgumentException("This Business Attribute already exists!");
            }

            if (BusinessAttributeUtils.hasNameConflict(input.getName(), context, _entityClient)) {
              throw new DataHubGraphQLException(
                  String.format(
                      "\"%s\" already exists as Business Attribute. Please pick a unique name.",
                      input.getName()),
                  DataHubGraphQLErrorCode.CONFLICT);
            }

            // Create the MCP
            final MetadataChangeProposal changeProposal =
                buildMetadataChangeProposalWithKey(
                    businessAttributeKey,
                    BUSINESS_ATTRIBUTE_ENTITY_NAME,
                    BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME,
                    mapBusinessAttributeInfo(input, context));

            // Ingest the MCP
            Urn businessAttributeUrn =
                UrnUtils.getUrn(
                    _entityClient.ingestProposal(context.getOperationContext(), changeProposal));
            OwnerUtils.addCreatorAsOwner(
                context,
                businessAttributeUrn.toString(),
                OwnerEntityType.CORP_USER,
                _entityService);
            return BusinessAttributeMapper.map(
                context,
                businessAttributeService.getBusinessAttributeEntityResponse(
                    context.getOperationContext(), businessAttributeUrn));

          } catch (DataHubGraphQLException e) {
            throw e;
          } catch (Exception e) {
            log.error(
                "Failed to create Business Attribute with name: {}: {}",
                input.getName(),
                e.getMessage());
            throw new RuntimeException(
                String.format("Failed to create Business Attribute with name: %s", input.getName()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private BusinessAttributeInfo mapBusinessAttributeInfo(
      CreateBusinessAttributeInput input, QueryContext context) {
    final BusinessAttributeInfo info = new BusinessAttributeInfo();
    info.setFieldPath(input.getName(), SetMode.DISALLOW_NULL);
    info.setName(input.getName(), SetMode.DISALLOW_NULL);
    info.setDescription(input.getDescription(), SetMode.IGNORE_NULL);
    info.setType(
        BusinessAttributeUtils.mapSchemaFieldDataType(input.getType()), SetMode.IGNORE_NULL);
    info.setCreated(
        new AuditStamp()
            .setActor(UrnUtils.getUrn(context.getActorUrn()))
            .setTime(System.currentTimeMillis()));
    info.setLastModified(
        new AuditStamp()
            .setActor(UrnUtils.getUrn(context.getActorUrn()))
            .setTime(System.currentTimeMillis()));
    return info;
  }
}
