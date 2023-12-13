package com.linkedin.datahub.graphql.types.glossary;

import static com.linkedin.metadata.Constants.GLOSSARY_NODE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_NODE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_NODE_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.GlossaryNode;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryNodeMapper;
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

public class GlossaryNodeType
    implements com.linkedin.datahub.graphql.types.EntityType<GlossaryNode, String> {

  static final Set<String> ASPECTS_TO_RESOLVE =
      ImmutableSet.of(
          GLOSSARY_NODE_KEY_ASPECT_NAME, GLOSSARY_NODE_INFO_ASPECT_NAME, OWNERSHIP_ASPECT_NAME);

  private final EntityClient _entityClient;

  public GlossaryNodeType(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public Class<GlossaryNode> objectClass() {
    return GlossaryNode.class;
  }

  @Override
  public EntityType type() {
    return EntityType.GLOSSARY_NODE;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public List<DataFetcherResult<GlossaryNode>> batchLoad(
      final List<String> urns, final QueryContext context) {
    final List<Urn> glossaryNodeUrns =
        urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    try {
      final Map<Urn, EntityResponse> glossaryNodeMap =
          _entityClient.batchGetV2(
              GLOSSARY_NODE_ENTITY_NAME,
              new HashSet<>(glossaryNodeUrns),
              ASPECTS_TO_RESOLVE,
              context.getAuthentication());

      final List<EntityResponse> gmsResults = new ArrayList<>();
      for (Urn urn : glossaryNodeUrns) {
        gmsResults.add(glossaryNodeMap.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(
              gmsGlossaryNode ->
                  gmsGlossaryNode == null
                      ? null
                      : DataFetcherResult.<GlossaryNode>newResult()
                          .data(GlossaryNodeMapper.map(gmsGlossaryNode))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load GlossaryNodes", e);
    }
  }
}
