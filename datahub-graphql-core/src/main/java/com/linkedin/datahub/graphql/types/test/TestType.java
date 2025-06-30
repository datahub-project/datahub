package com.linkedin.datahub.graphql.types.test;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Test;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.execution.DataFetcherResult;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

@Slf4j
public class TestType implements com.linkedin.datahub.graphql.types.EntityType<Test, String> {

  static final Set<String> ASPECTS_TO_FETCH = ImmutableSet.of(Constants.TEST_INFO_ASPECT_NAME);
  private final EntityClient _entityClient;

  public TestType(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public EntityType type() {
    return EntityType.TEST;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<Test> objectClass() {
    return Test.class;
  }

  @Override
  public List<DataFetcherResult<Test>> batchLoad(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final List<Urn> testUrns = urns.stream().map(this::getUrn).collect(Collectors.toList());

    try {
      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(
              context.getOperationContext(),
              Constants.TEST_ENTITY_NAME,
              new HashSet<>(testUrns),
              ASPECTS_TO_FETCH);

      final List<EntityResponse> gmsResults = new ArrayList<>(urns.size());
      for (Urn urn : testUrns) {
        gmsResults.add(entities.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(
              gmsResult -> {
                DataFetcherResult<Test> result =
                    gmsResult == null
                        ? null
                        : DataFetcherResult.<Test>newResult()
                            .data(TestMapper.map(gmsResult))
                            .build();
                if (result == null || result.getData() == null) {
                  String urn =
                      gmsResult != null ? gmsResult.getUrn().toString() : StringUtils.EMPTY;
                  // Only have access to urn from result here, so log and then exclude with filter
                  // after
                  // TODO: Ideally this would be done at the top level in GMSGraphQLEngine for all
                  // batch loaders, but
                  //      need to evaluate performance implications before adding in widespread.
                  // Start with small scope
                  log.warn("Excluding empty result for urn: {}", urn);
                }
                return result;
              })
          .filter(result -> result != null && result.getData() != null)
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Tests", e);
    }
  }

  private Urn getUrn(final String urnStr) {
    try {
      return Urn.createFromString(urnStr);
    } catch (URISyntaxException e) {
      throw new RuntimeException(String.format("Failed to convert urn string %s into Urn", urnStr));
    }
  }
}
