package com.linkedin.datahub.graphql.resolvers.businessattribute;

import com.linkedin.businessattribute.BusinessAttributeInfo;
import com.linkedin.businessattribute.BusinessAttributeKey;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.CreateBusinessAttributeInput;
import com.linkedin.datahub.graphql.generated.OwnerEntityType;
import com.linkedin.datahub.graphql.generated.OwnershipType;
import com.linkedin.datahub.graphql.resolvers.mutate.util.BusinessAttributeUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.OwnerUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.buildMetadataChangeProposalWithKey;
import static com.linkedin.datahub.graphql.resolvers.mutate.util.OwnerUtils.mapOwnershipTypeToEntity;
import static com.linkedin.metadata.Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME;

@Slf4j
@RequiredArgsConstructor
public class CreateBusinessAttributeResolver implements DataFetcher<CompletableFuture<String>> {
    private final EntityClient _entityClient;
    private final EntityService _entityService;

    @Override
    public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {
        final QueryContext context = environment.getContext();
        CreateBusinessAttributeInput input = bindArgument(environment.getArgument("input"), CreateBusinessAttributeInput.class);

        return CompletableFuture.supplyAsync(() -> {
            if (!BusinessAttributeAuthorizationUtils.canCreateBusinessAttribute(context)) {
                throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
            }

            try {
                final BusinessAttributeKey businessAttributeKey = new BusinessAttributeKey();
                businessAttributeKey.setId(UUID.randomUUID().toString());

                if (_entityClient.exists(EntityKeyUtils.convertEntityKeyToUrn(businessAttributeKey,
                                BUSINESS_ATTRIBUTE_ENTITY_NAME),
                        context.getAuthentication())) {
                    throw new IllegalArgumentException("This Business Attribute already exists!");
                }

                if (BusinessAttributeUtils.hasNameConflict(input.getBusinessAttributeInfo().getName(), context, _entityClient)) {
                    throw new DataHubGraphQLException(
                            String.format("\"%s\" already exists as Business Attribute. Please pick a unique name.",
                                    input.getBusinessAttributeInfo().getName()), DataHubGraphQLErrorCode.CONFLICT);
                }

                // Create the MCP
                final MetadataChangeProposal changeProposal = buildMetadataChangeProposalWithKey(
                        businessAttributeKey, BUSINESS_ATTRIBUTE_ENTITY_NAME,
                        BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME,
                        mapBusinessAttributeInfo(input, context)
                );

                // Ingest the MCP
                String businessAttributeUrn = _entityClient.ingestProposal(changeProposal, context.getAuthentication());
                OwnershipType ownershipType = OwnershipType.TECHNICAL_OWNER;
                if (!_entityService.exists(UrnUtils.getUrn(mapOwnershipTypeToEntity(ownershipType.name())))) {
                    log.warn("Technical owner does not exist, defaulting to None ownership.");
                    ownershipType = OwnershipType.NONE;
                }
                OwnerUtils.addCreatorAsOwner(context, businessAttributeUrn, OwnerEntityType.CORP_USER, ownershipType, _entityService);
                return businessAttributeUrn;

            } catch (DataHubGraphQLException e) {
                throw e;
            } catch (Exception e) {
                log.error("Failed to create Business Attribute with name: {}: {}", input.getBusinessAttributeInfo().getName(), e.getMessage());
                throw new RuntimeException(String.format("Failed to create Business Attribute with name: %s", input.getBusinessAttributeInfo().getName()), e);
            }
        });
    }

    private BusinessAttributeInfo mapBusinessAttributeInfo(CreateBusinessAttributeInput input, QueryContext context) {
        final BusinessAttributeInfo info = new BusinessAttributeInfo();
        info.setFieldPath(input.getBusinessAttributeInfo().getName(), SetMode.DISALLOW_NULL);
        info.setName(input.getBusinessAttributeInfo().getName(), SetMode.DISALLOW_NULL);
        info.setDescription(input.getBusinessAttributeInfo().getDescription(), SetMode.IGNORE_NULL);
        info.setType(BusinessAttributeUtils.mapSchemaFieldDataType(input.getBusinessAttributeInfo().getType()), SetMode.IGNORE_NULL);
        info.setCreated(new AuditStamp().setActor(UrnUtils.getUrn(context.getActorUrn())).setTime(System.currentTimeMillis()));
        return info;
    }

}
