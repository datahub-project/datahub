package com.linkedin.datahub.graphql.types.template;

import static com.linkedin.metadata.Constants.DATAHUB_PAGE_TEMPLATE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubPageTemplate;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import graphql.execution.DataFetcherResult;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class PageTemplateType
    implements com.linkedin.datahub.graphql.types.EntityType<DataHubPageTemplate, String> {
  public static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME);
  private final EntityClient _entityClient;

  @Override
  public EntityType type() {
    return EntityType.DATAHUB_PAGE_TEMPLATE;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<DataHubPageTemplate> objectClass() {
    return DataHubPageTemplate.class;
  }

  @Override
  public List<DataFetcherResult<DataHubPageTemplate>> batchLoad(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final List<Urn> applicationUrns =
        urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    try {
      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(
              context.getOperationContext(),
              DATAHUB_PAGE_TEMPLATE_ENTITY_NAME,
              new HashSet<>(applicationUrns),
              ASPECTS_TO_FETCH);

      final List<EntityResponse> gmsResults = new ArrayList<>(urns.size());
      for (Urn urn : applicationUrns) {
        gmsResults.add(entities.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(
              gmsResult ->
                  gmsResult == null
                      ? null
                      : DataFetcherResult.<DataHubPageTemplate>newResult()
                          .data(PageTemplateMapper.map(context, gmsResult))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Applications", e);
    }
  }
}
