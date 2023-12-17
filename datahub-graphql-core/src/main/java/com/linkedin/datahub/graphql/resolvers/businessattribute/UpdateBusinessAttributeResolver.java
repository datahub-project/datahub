package com.linkedin.datahub.graphql.resolvers.businessattribute;

import com.datahub.authentication.Authentication;
import com.linkedin.businessattribute.BusinessAttributeInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.BusinessAttribute;
import com.linkedin.datahub.graphql.generated.UpdateBusinessAttributeInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.BusinessAttributeUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

@Slf4j
@RequiredArgsConstructor
public class UpdateBusinessAttributeResolver implements DataFetcher<CompletableFuture<BusinessAttribute>> {

    private final EntityClient _entityClient;

    @Override
    public CompletableFuture<BusinessAttribute> get(DataFetchingEnvironment environment) throws Exception {
        QueryContext context = environment.getContext();
        UpdateBusinessAttributeInput input = bindArgument(environment.getArgument("input"), UpdateBusinessAttributeInput.class);
        final Urn businessAttributeUrn = UrnUtils.getUrn(environment.getArgument("urn"));
        if (!BusinessAttributeAuthorizationUtils.canCreateBusinessAttribute(context)) {
            throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                if (!_entityClient.exists(businessAttributeUrn, context.getAuthentication())) {
                    throw new IllegalArgumentException("The Business Attribute provided dos not exist");
                }
                updateBusinessAttribute(input, businessAttributeUrn, context);
            } catch (DataHubGraphQLException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(String.format("Failed to update Business Attribute with urn %s", businessAttributeUrn), e);
            }
            return null;
        });
    }

    private Urn updateBusinessAttribute(UpdateBusinessAttributeInput input, Urn businessAttributeUrn, QueryContext context) {
        try {
            BusinessAttributeInfo businessAttributeInfo = getBusinessAttributeInfo(businessAttributeUrn, context.getAuthentication());
            // 1. Check whether the Business Attribute exists
            if (businessAttributeInfo == null) {
                throw new IllegalArgumentException(
                        String.format("Failed to update Business Attribute. Business Attribute with urn %s does not exist.", businessAttributeUrn));
            }

            // 2. Apply changes to existing Business Attribute
            if (Objects.nonNull(input.getName())) {
                if (BusinessAttributeUtils.hasNameConflict(input.getName(), context, _entityClient)) {
                    throw new DataHubGraphQLException(
                        String.format("\"%s\" already exists as Business Attribute. Please pick a unique name.", input.getName()),
                            DataHubGraphQLErrorCode.CONFLICT);
                }
                businessAttributeInfo.setName(input.getName());
                businessAttributeInfo.setFieldPath(input.getName());
            }
            if (Objects.nonNull(input.getDescription())) {
                businessAttributeInfo.setDescription(input.getDescription());
            }
            if (Objects.nonNull(input.getType())) {
                businessAttributeInfo.setType(BusinessAttributeUtils.mapSchemaFieldDataType(input.getType()));
            }
            // 3. Write changes to GMS
            return UrnUtils.getUrn(_entityClient.ingestProposal(
                            AspectUtils.buildMetadataChangeProposal(
                                    businessAttributeUrn, Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME, businessAttributeInfo), context.getAuthentication()
                    )
            );

        } catch (DataHubGraphQLException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Nullable
    public BusinessAttributeInfo getBusinessAttributeInfo(@Nonnull final Urn businessAttributeUrn, @Nonnull final Authentication authentication) {
        Objects.requireNonNull(businessAttributeUrn, "businessAttributeUrn must not be null");
        Objects.requireNonNull(authentication, "authentication must not be null");
        final EntityResponse response = getBusinessAttributeEntityResponse(businessAttributeUrn, authentication);
        if (response != null && response.getAspects().containsKey(Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME)) {
            return new BusinessAttributeInfo(response.getAspects().get(Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME).getValue().data());
        }
        // No aspect found
        return null;
    }

    private EntityResponse getBusinessAttributeEntityResponse(@Nonnull final Urn businessAttributeUrn, @Nonnull final Authentication authentication) {
        Objects.requireNonNull(businessAttributeUrn, "business attribute must not be null");
        Objects.requireNonNull(authentication, "authentication must not be null");
        try {
            return _entityClient.batchGetV2(
                    Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME,
                    Set.of(businessAttributeUrn),
                    Set.of(Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME),
                    authentication).get(businessAttributeUrn);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to retrieve Business Attribute with urn %s", businessAttributeUrn), e);
        }
    }
}
