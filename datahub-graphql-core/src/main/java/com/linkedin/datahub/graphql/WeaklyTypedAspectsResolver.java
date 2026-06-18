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
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.dataloader.BatchLoaderContextProvider;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;

@Slf4j
public class WeaklyTypedAspectsResolver implements DataFetcher<CompletableFuture<List<RawAspect>>> {

  public static final String LOADER_NAME = "WeaklyTypedAspects";

  private static final JacksonDataCodec CODEC = new JacksonDataCodec();

  @Override
  public CompletableFuture<List<RawAspect>> get(DataFetchingEnvironment environment) {
    final String urnStr = ((Entity) environment.getSource()).getUrn();
    final EntityType entityType = ((Entity) environment.getSource()).getType();
    final String entityTypeName = EntityTypeMapper.getName(entityType);
    final AspectParams input = bindArgument(environment.getArgument("input"), AspectParams.class);
    final DataLoader<AspectsKey, List<RawAspect>> loader =
        environment.getDataLoaderRegistry().getDataLoader(LOADER_NAME);
    return loader.load(AspectsKey.from(urnStr, entityTypeName, input));
  }

  public static DataLoader<AspectsKey, List<RawAspect>> createDataLoader(
      final EntityClient entityClient,
      final EntityRegistry entityRegistry,
      final QueryContext queryContext) {
    final BatchLoaderContextProvider provider = () -> queryContext;
    final DataLoaderOptions options =
        DataLoaderOptions.newOptions().setBatchLoaderContextProvider(provider);
    return DataLoader.newDataLoader(
        (keys, env) ->
            GraphQLConcurrencyUtils.supplyAsync(
                () ->
                    batchLoad(keys, (QueryContext) env.getContext(), entityClient, entityRegistry),
                LOADER_NAME,
                "batchLoad"),
        options);
  }

  /**
   * Batch-resolves a list of {@link AspectsKey}s. Keys are grouped by {@code (entityType,
   * aspectNames, autoRenderOnly)} so that one {@code batchGetV2} call serves all URNs in a group.
   * Returns a list of results in the same order as the input keys (DataLoader positional contract).
   */
  public static List<List<RawAspect>> batchLoad(
      final List<AspectsKey> keys,
      final QueryContext context,
      final EntityClient entityClient,
      final EntityRegistry entityRegistry) {
    final OperationContext opContext = context.getOperationContext();
    final List<List<RawAspect>> results = new ArrayList<>(Collections.nCopies(keys.size(), null));

    final Map<GroupKey, List<Integer>> groups = new HashMap<>();
    for (int i = 0; i < keys.size(); i++) {
      final AspectsKey k = keys.get(i);
      final GroupKey gk = new GroupKey(k.getEntityType(), k.getAspectNames(), k.isAutoRenderOnly());
      groups.computeIfAbsent(gk, g -> new ArrayList<>()).add(i);
    }

    for (Map.Entry<GroupKey, List<Integer>> entry : groups.entrySet()) {
      final GroupKey gk = entry.getKey();
      final List<Integer> indices = entry.getValue();
      final EntitySpec entitySpec = entityRegistry.getEntitySpec(gk.getEntityType());
      final Set<String> aspectsToFetch =
          computeAspectsToFetch(entitySpec, gk.getAspectNames(), gk.isAutoRenderOnly());

      if (aspectsToFetch.isEmpty()) {
        for (int idx : indices) {
          results.set(idx, new ArrayList<>());
        }
        continue;
      }

      try {
        final Set<Urn> urns = new HashSet<>();
        final Map<Integer, Urn> indexToUrn = new HashMap<>();
        for (int idx : indices) {
          final Urn urn = Urn.createFromString(keys.get(idx).getUrn());
          urns.add(urn);
          indexToUrn.put(idx, urn);
        }
        final Map<Urn, EntityResponse> responses =
            entityClient.batchGetV2(opContext, gk.getEntityType(), urns, aspectsToFetch);
        for (int idx : indices) {
          results.set(
              idx,
              buildRawAspectList(responses.get(indexToUrn.get(idx)), entitySpec, aspectsToFetch));
        }
      } catch (RemoteInvocationException | URISyntaxException e) {
        throw new RuntimeException(
            "Failed to batch-fetch aspects for entity type " + gk.getEntityType(), e);
      }
    }
    return results;
  }

  private static Set<String> computeAspectsToFetch(
      final EntitySpec entitySpec, final Set<String> requestedNames, final boolean autoRenderOnly) {
    return entitySpec.getAspectSpecs().stream()
        .filter(spec -> matchesParams(spec, requestedNames, autoRenderOnly))
        .map(AspectSpec::getName)
        .collect(Collectors.toSet());
  }

  private static boolean matchesParams(
      final AspectSpec spec, final Set<String> requestedNames, final boolean autoRenderOnly) {
    return (!autoRenderOnly || spec.isAutoRender())
        && (requestedNames.isEmpty() || requestedNames.contains(spec.getName()));
  }

  private static List<RawAspect> buildRawAspectList(
      final EntityResponse response,
      final EntitySpec entitySpec,
      final Set<String> aspectsToFetch) {
    final List<RawAspect> out = new ArrayList<>();
    if (response == null) {
      return out;
    }
    for (AspectSpec spec : entitySpec.getAspectSpecs()) {
      if (!aspectsToFetch.contains(spec.getName())) {
        continue;
      }
      if (!response.getAspects().containsKey(spec.getName())) {
        continue;
      }
      final DataMap resolvedAspect = response.getAspects().get(spec.getName()).getValue().data();
      if (resolvedAspect == null) {
        continue;
      }
      final RawAspect raw = new RawAspect();
      try {
        raw.setPayload(CODEC.mapToString(resolvedAspect));
      } catch (IOException e) {
        throw new RuntimeException("Failed to serialize aspect " + spec.getName(), e);
      }
      raw.setAspectName(spec.getName());
      final DataMap renderSpec = spec.getRenderSpec();
      if (renderSpec != null) {
        final AspectRenderSpec resultRenderSpec = new AspectRenderSpec();
        resultRenderSpec.setDisplayType(renderSpec.getString("displayType"));
        resultRenderSpec.setDisplayName(renderSpec.getString("displayName"));
        resultRenderSpec.setKey(renderSpec.getString("key"));
        raw.setRenderSpec(resultRenderSpec);
      }
      out.add(raw);
    }
    return out;
  }

  @Value
  public static class AspectsKey {
    String urn;
    String entityType;
    Set<String> aspectNames;
    boolean autoRenderOnly;

    public static AspectsKey from(
        final String urn, final String entityType, final AspectParams params) {
      final Set<String> names =
          (params == null || params.getAspectNames() == null)
              ? Collections.emptySet()
              : new HashSet<>(params.getAspectNames());
      final boolean autoOnly = params != null && Boolean.TRUE.equals(params.getAutoRenderOnly());
      return new AspectsKey(urn, entityType, names, autoOnly);
    }
  }

  @Value
  private static class GroupKey {
    String entityType;
    Set<String> aspectNames;
    boolean autoRenderOnly;
  }
}
