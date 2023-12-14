package com.linkedin.datahub.graphql.resolvers.glossary;

import static com.linkedin.metadata.Constants.GLOSSARY_NODE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERM_ENTITY_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERM_INFO_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.GlossaryNode;
import com.linkedin.datahub.graphql.generated.ParentNodesResult;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryNodeMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ParentNodesResolver implements DataFetcher<CompletableFuture<ParentNodesResult>> {

  private final EntityClient _entityClient;

  public ParentNodesResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  private void aggregateParentNodes(List<GlossaryNode> nodes, String urn, QueryContext context) {
    try {
      Urn entityUrn = new Urn(urn);
      EntityResponse entityResponse =
          _entityClient.getV2(
              entityUrn.getEntityType(),
              entityUrn,
              Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME),
              context.getAuthentication());

      if (entityResponse != null
          && entityResponse.getAspects().containsKey(GLOSSARY_NODE_INFO_ASPECT_NAME)) {
        DataMap dataMap =
            entityResponse.getAspects().get(GLOSSARY_NODE_INFO_ASPECT_NAME).getValue().data();
        GlossaryNodeInfo nodeInfo = new GlossaryNodeInfo(dataMap);
        if (nodeInfo.hasParentNode()) {
          Urn parentNodeUrn = nodeInfo.getParentNode();
          EntityResponse response =
              _entityClient.getV2(
                  parentNodeUrn.getEntityType(), parentNodeUrn, null, context.getAuthentication());
          if (response != null) {
            GlossaryNode mappedNode = GlossaryNodeMapper.map(response);
            nodes.add(mappedNode);
            aggregateParentNodes(nodes, mappedNode.getUrn(), context);
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to retrieve parent nodes from GMS", e);
    }
  }

  private GlossaryNode getTermParentNode(String urn, QueryContext context) {
    try {
      Urn entityUrn = new Urn(urn);
      EntityResponse entityResponse =
          _entityClient.getV2(
              entityUrn.getEntityType(),
              entityUrn,
              Collections.singleton(GLOSSARY_TERM_INFO_ASPECT_NAME),
              context.getAuthentication());

      if (entityResponse != null
          && entityResponse.getAspects().containsKey(GLOSSARY_TERM_INFO_ASPECT_NAME)) {
        DataMap dataMap =
            entityResponse.getAspects().get(GLOSSARY_TERM_INFO_ASPECT_NAME).getValue().data();
        GlossaryTermInfo termInfo = new GlossaryTermInfo(dataMap);
        if (termInfo.hasParentNode()) {
          Urn parentNodeUrn = termInfo.getParentNode();
          EntityResponse response =
              _entityClient.getV2(
                  parentNodeUrn.getEntityType(), parentNodeUrn, null, context.getAuthentication());
          if (response != null) {
            GlossaryNode mappedNode = GlossaryNodeMapper.map(response);
            return mappedNode;
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to get glossary term parent node from GMS", e);
    }
    return null;
  }

  @Override
  public CompletableFuture<ParentNodesResult> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final String urn = ((Entity) environment.getSource()).getUrn();
    final List<GlossaryNode> nodes = new ArrayList<>();

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            final String type = Urn.createFromString(urn).getEntityType();

            if (GLOSSARY_TERM_ENTITY_NAME.equals(type)) {
              final GlossaryNode parentNode = getTermParentNode(urn, context);
              if (parentNode != null) {
                nodes.add(parentNode);
                aggregateParentNodes(nodes, parentNode.getUrn(), context);
              }
            } else {
              aggregateParentNodes(nodes, urn, context);
            }

            final ParentNodesResult result = new ParentNodesResult();
            result.setCount(nodes.size());
            result.setNodes(nodes);
            return result;
          } catch (DataHubGraphQLException | URISyntaxException e) {
            throw new RuntimeException(("Failed to load parent nodes"));
          }
        });
  }
}
