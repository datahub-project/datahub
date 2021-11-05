package com.linkedin.datahub.graphql;

import com.linkedin.data.DataMap;
import com.linkedin.data.codec.JacksonDataCodec;
import com.linkedin.datahub.graphql.generated.DynamicAspectResult;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.resolvers.EntityTypeMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@AllArgsConstructor
public class DynamicAspectsResolver implements DataFetcher<CompletableFuture<List<DynamicAspectResult>>> {

    EntityClient _aspectClient;
    EntityRegistry _entityRegistry;
    private static final JacksonDataCodec CODEC = new JacksonDataCodec();

    @Override
    public CompletableFuture<List<DynamicAspectResult>> get(DataFetchingEnvironment environment) throws Exception {
        return CompletableFuture.supplyAsync(() -> {
            List<DynamicAspectResult> results = new ArrayList<>();

            final QueryContext context = environment.getContext();
            final String urn = ((Entity) environment.getSource()).getUrn();
            final EntityType entityType = ((Entity) environment.getSource()).getType();
            final String entityTypeName = EntityTypeMapper.getName(entityType);
            EntitySpec entitySpec = _entityRegistry.getEntitySpec(entityTypeName);
            entitySpec.getAspectSpecs().stream().filter(aspectSpec -> aspectSpec.isAutoRender()).forEach(aspectSpec -> {
                try {
                    DynamicAspectResult result = new DynamicAspectResult();
                    DataMap resolvedAspect =
                        _aspectClient.getRawAspect(urn, aspectSpec.getName(), 0L, context.getActor());
                    if (resolvedAspect == null) {
                        return;
                    }

                    result.setPayload(CODEC.mapToString(resolvedAspect));
                    result.setAspectName(aspectSpec.getName());

                    DataMap renderSpec = aspectSpec.getRenderSpec();

                    result.setDisplayType(renderSpec.getString("displayType"));
                    result.setDisplayName(renderSpec.getString("displayName"));

                    results.add(result);
                } catch (RemoteInvocationException e) {
                    throw new RuntimeException("Failed to fetch aspect " + aspectSpec.getName() + " for urn " + urn + " ", e);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            return results;
        });
    }
}
