package com.linkedin.datahub.graphql;

import com.linkedin.data.DataMap;

import com.linkedin.data.codec.JacksonDataCodec;
import com.linkedin.datahub.graphql.generated.AspectParams;
import com.linkedin.datahub.graphql.generated.AspectRenderSpec;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.RawAspect;
import com.linkedin.datahub.graphql.resolvers.EntityTypeMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.models.AspectSpec;
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

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


@Slf4j
@AllArgsConstructor
public class WeaklyTypedAspectsResolver implements DataFetcher<CompletableFuture<List<RawAspect>>> {

    private final EntityClient _entityClient;
    private final EntityRegistry _entityRegistry;
    private static final JacksonDataCodec CODEC = new JacksonDataCodec();

    private boolean shouldReturnAspect(AspectSpec aspectSpec, AspectParams params) {
      return !params.getAutoRenderOnly() || aspectSpec.isAutoRender();
    }

    @Override
    public CompletableFuture<List<RawAspect>> get(DataFetchingEnvironment environment) throws Exception {
        return CompletableFuture.supplyAsync(() -> {
            List<RawAspect> results = new ArrayList<>();

            final QueryContext context = environment.getContext();
            final String urn = ((Entity) environment.getSource()).getUrn();
            final EntityType entityType = ((Entity) environment.getSource()).getType();
            final String entityTypeName = EntityTypeMapper.getName(entityType);
            final AspectParams input = bindArgument(environment.getArgument("input"), AspectParams.class);

            EntitySpec entitySpec = _entityRegistry.getEntitySpec(entityTypeName);
            entitySpec.getAspectSpecs().stream().filter(aspectSpec -> shouldReturnAspect(aspectSpec, input)).forEach(aspectSpec -> {
                try {
                    RawAspect result = new RawAspect();
                    DataMap resolvedAspect =
                        _entityClient.getRawAspect(urn, aspectSpec.getName(), 0L, context.getAuthentication());
                    if (resolvedAspect == null || resolvedAspect.keySet().size() != 1) {
                        return;
                    }

                    DataMap aspectPayload = resolvedAspect.getDataMap(resolvedAspect.keySet().iterator().next());

                    result.setPayload(CODEC.mapToString(aspectPayload));
                    result.setAspectName(aspectSpec.getName());

                    DataMap renderSpec = aspectSpec.getRenderSpec();

                    AspectRenderSpec resultRenderSpec = new AspectRenderSpec();

                    resultRenderSpec.setDisplayType(renderSpec.getString("displayType"));
                    resultRenderSpec.setDisplayName(renderSpec.getString("displayName"));
                    resultRenderSpec.setKey(renderSpec.getString("key"));
                    result.setRenderSpec(resultRenderSpec);

                    results.add(result);
                } catch (IOException | RemoteInvocationException e) {
                    throw new RuntimeException("Failed to fetch aspect " + aspectSpec.getName() + " for urn " + urn + " ", e);
                }
            });
            return results;
        });
    }
}
