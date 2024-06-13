package com.linkedin.datahub.graphql;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.codec.JacksonDataCodec;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.AspectParams;
import com.linkedin.datahub.graphql.generated.AspectRenderSpec;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.RawAspect;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class WeaklyTypedAspectsResolver implements DataFetcher<CompletableFuture<List<RawAspect>>> {

  private final EntityClient _entityClient;
  private final EntityRegistry _entityRegistry;
  private static final JacksonDataCodec CODEC = new JacksonDataCodec();

  private boolean shouldReturnAspect(AspectSpec aspectSpec, AspectParams params) {
    return (params.getAutoRenderOnly() == null
            || !params.getAutoRenderOnly()
            || aspectSpec.isAutoRender())
        && (params.getAspectNames() == null
            || params.getAspectNames().isEmpty()
            || params.getAspectNames().contains(aspectSpec.getName()));
  }

  @Override
  public CompletableFuture<List<RawAspect>> get(DataFetchingEnvironment environment)
      throws Exception {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          List<RawAspect> results = new ArrayList<>();

          final QueryContext context = environment.getContext();
          final String urnStr = ((Entity) environment.getSource()).getUrn();
          final EntityType entityType = ((Entity) environment.getSource()).getType();
          final String entityTypeName = EntityTypeMapper.getName(entityType);
          final AspectParams input =
              bindArgument(environment.getArgument("input"), AspectParams.class);

          EntitySpec entitySpec = _entityRegistry.getEntitySpec(entityTypeName);
          entitySpec.getAspectSpecs().stream()
              .filter(aspectSpec -> shouldReturnAspect(aspectSpec, input))
              .forEach(
                  aspectSpec -> {
                    try {
                      Urn urn = Urn.createFromString(urnStr);
                      RawAspect result = new RawAspect();
                      EntityResponse entityResponse =
                          _entityClient
                              .batchGetV2(
                                  context.getOperationContext(),
                                  urn.getEntityType(),
                                  Collections.singleton(urn),
                                  Collections.singleton(aspectSpec.getName()))
                              .get(urn);
                      if (entityResponse == null
                          || !entityResponse.getAspects().containsKey(aspectSpec.getName())) {
                        return;
                      }

                      DataMap resolvedAspect =
                          entityResponse.getAspects().get(aspectSpec.getName()).getValue().data();
                      if (resolvedAspect == null) {
                        return;
                      }

                      result.setPayload(CODEC.mapToString(resolvedAspect));
                      result.setAspectName(aspectSpec.getName());

                      DataMap renderSpec = aspectSpec.getRenderSpec();

                      if (renderSpec != null) {
                        AspectRenderSpec resultRenderSpec = new AspectRenderSpec();
                        resultRenderSpec.setDisplayType(renderSpec.getString("displayType"));
                        resultRenderSpec.setDisplayName(renderSpec.getString("displayName"));
                        resultRenderSpec.setKey(renderSpec.getString("key"));
                        result.setRenderSpec(resultRenderSpec);
                      }

                      results.add(result);
                    } catch (IOException | RemoteInvocationException | URISyntaxException e) {
                      throw new RuntimeException(
                          "Failed to fetch aspect "
                              + aspectSpec.getName()
                              + " for urn "
                              + urnStr
                              + " ",
                          e);
                    }
                  });
          return results;
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
