package com.linkedin.datahub.graphql.resolvers.businessattribute;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

/**
 * Resolver responsible for hard deleting a particular Business Attribute
 */
@Slf4j
@RequiredArgsConstructor
public class DeleteBusinessAttributeResolver implements DataFetcher<CompletableFuture<Boolean>> {
    private final EntityClient _entityClient;

    @Override
    public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
        final QueryContext context = environment.getContext();
        final Urn businessAttributeUrn = UrnUtils.getUrn(environment.getArgument("urn"));
        return CompletableFuture.supplyAsync(() -> {
            if (!BusinessAttributeAuthorizationUtils.canManageBusinessAttribute(context)) {
                throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
            }
            try {
                if (!_entityClient.exists(businessAttributeUrn, context.getAuthentication())) {
                    throw new IllegalArgumentException("The Business Attribute provided dos not exist");
                }
                _entityClient.deleteEntity(businessAttributeUrn, context.getAuthentication());
                CompletableFuture.runAsync(() -> {
                    try {
                        _entityClient.deleteEntityReferences(businessAttributeUrn, context.getAuthentication());
                    } catch (Exception e) {
                        log.error(String.format(
                            "Exception while attempting to clear all entity references for Business Attribute with urn %s", businessAttributeUrn), e);
                    }
                });
                return true;
            } catch (Exception e) {
                throw new RuntimeException(String.format("Failed to delete Business Attribute with urn %s", businessAttributeUrn), e);
            }
        });
    }
}
