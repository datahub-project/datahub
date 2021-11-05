package com.linkedin.datahub.graphql;

import com.linkedin.common.SubTypes;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.DynamicAspectResult;
import com.linkedin.datahub.graphql.generated.DynamicAspectsInput;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.TagAssociationInput;
import com.linkedin.entity.client.AspectClient;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


@Slf4j
@AllArgsConstructor
public class DynamicAspectsResolver implements DataFetcher<CompletableFuture<List<DynamicAspectResult>>> {

    EntityClient _aspectClient;

    @Override
    public CompletableFuture<List<DynamicAspectResult>> get(DataFetchingEnvironment environment) throws Exception {
        final DynamicAspectsInput input = bindArgument(environment.getArgument("input"), DynamicAspectsInput.class);
        return CompletableFuture.supplyAsync(() -> {
            List<DynamicAspectResult> results = new ArrayList<>();

            final QueryContext context = environment.getContext();
            final String urn = ((Entity) environment.getSource()).getUrn();

            for (String aspectName : input.getAspects()) {
                try {
                    DynamicAspectResult result = new DynamicAspectResult();
                    DataMap resolvedAspect =
                        _aspectClient.getRawAspect(urn, aspectName, 0L, context.getActor());
                    result.setPayload(resolvedAspect.toString());
                    result.setAspectName(aspectName);
                    result.setDisplayType("TODO");
                    results.add(result);
                } catch (RemoteInvocationException e) {
                    throw new RuntimeException("Failed to fetch aspect " + aspectName + " for urn " + urn + " ", e);
                }
            }
            return results;
        });
    }
}
