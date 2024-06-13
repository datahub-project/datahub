package com.linkedin.datahub.graphql.resolvers.glossary;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateGlossaryEntityInput;
import com.linkedin.datahub.graphql.generated.OwnerEntityType;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.OwnerUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.GlossaryTermKey;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class CreateGlossaryTermResolver implements DataFetcher<CompletableFuture<String>> {

  static final String PARENT_NODE_INDEX_FIELD_NAME = "parentNode.keyword";

  private final EntityClient _entityClient;
  private final EntityService _entityService;

  @Override
  public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final CreateGlossaryEntityInput input =
        bindArgument(environment.getArgument("input"), CreateGlossaryEntityInput.class);
    final Urn parentNode =
        input.getParentNode() != null ? UrnUtils.getUrn(input.getParentNode()) : null;

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (GlossaryUtils.canManageChildrenEntities(context, parentNode, _entityClient)) {
            // Ensure there isn't another glossary term with the same name at this level of the
            // glossary
            validateGlossaryTermName(parentNode, context, input.getName());
            try {
              final GlossaryTermKey key = new GlossaryTermKey();

              final String id =
                  input.getId() != null ? input.getId() : UUID.randomUUID().toString();
              key.setName(id);

              if (_entityClient.exists(
                  context.getOperationContext(),
                  EntityKeyUtils.convertEntityKeyToUrn(key, GLOSSARY_TERM_ENTITY_NAME))) {
                throw new IllegalArgumentException("This Glossary Term already exists!");
              }

              final MetadataChangeProposal proposal =
                  buildMetadataChangeProposalWithKey(
                      key,
                      GLOSSARY_TERM_ENTITY_NAME,
                      GLOSSARY_TERM_INFO_ASPECT_NAME,
                      mapGlossaryTermInfo(input));

              String glossaryTermUrn =
                  _entityClient.ingestProposal(context.getOperationContext(), proposal, false);

              OwnerUtils.addCreatorAsOwner(
                  context, glossaryTermUrn, OwnerEntityType.CORP_USER, _entityService);
              return glossaryTermUrn;
            } catch (Exception e) {
              log.error(
                  "Failed to create GlossaryTerm with id: {}, name: {}: {}",
                  input.getId(),
                  input.getName(),
                  e.getMessage());
              throw new RuntimeException(
                  String.format(
                      "Failed to create GlossaryTerm with id: %s, name: %s",
                      input.getId(), input.getName()),
                  e);
            }
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private GlossaryTermInfo mapGlossaryTermInfo(final CreateGlossaryEntityInput input) {
    final GlossaryTermInfo result = new GlossaryTermInfo();
    result.setName(input.getName());
    result.setTermSource("INTERNAL");
    final String description = input.getDescription() != null ? input.getDescription() : "";
    result.setDefinition(description);
    if (input.getParentNode() != null) {
      try {
        final GlossaryNodeUrn parentNode = GlossaryNodeUrn.createFromString(input.getParentNode());
        result.setParentNode(parentNode, SetMode.IGNORE_NULL);
      } catch (URISyntaxException e) {
        throw new RuntimeException(
            String.format(
                "Failed to create GlossaryNodeUrn from string: %s", input.getParentNode()),
            e);
      }
    }
    return result;
  }

  private Filter buildParentNodeFilter(final Urn parentNodeUrn) {
    final Map<String, String> criterionMap = new HashMap<>();
    criterionMap.put(
        PARENT_NODE_INDEX_FIELD_NAME, parentNodeUrn == null ? null : parentNodeUrn.toString());
    return QueryUtils.newFilter(criterionMap);
  }

  private Map<Urn, EntityResponse> getTermsWithSameParent(Urn parentNode, QueryContext context) {
    try {
      final Filter filter = buildParentNodeFilter(parentNode);
      final SearchResult searchResult =
          _entityClient.filter(
              context.getOperationContext(), GLOSSARY_TERM_ENTITY_NAME, filter, null, 0, 1000);

      final List<Urn> termUrns =
          searchResult.getEntities().stream()
              .map(SearchEntity::getEntity)
              .collect(Collectors.toList());

      return _entityClient.batchGetV2(
          context.getOperationContext(),
          GLOSSARY_TERM_ENTITY_NAME,
          new HashSet<>(termUrns),
          Collections.singleton(GLOSSARY_TERM_INFO_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException("Failed fetching Glossary Terms with the same parent", e);
    }
  }

  private void validateGlossaryTermName(Urn parentNode, QueryContext context, String name) {
    Map<Urn, EntityResponse> entities = getTermsWithSameParent(parentNode, context);

    entities.forEach(
        (urn, entityResponse) -> {
          if (entityResponse.getAspects().containsKey(GLOSSARY_TERM_INFO_ASPECT_NAME)) {
            DataMap dataMap =
                entityResponse.getAspects().get(GLOSSARY_TERM_INFO_ASPECT_NAME).getValue().data();
            GlossaryTermInfo termInfo = new GlossaryTermInfo(dataMap);
            if (termInfo.hasName() && termInfo.getName().equals(name)) {
              throw new IllegalArgumentException(
                  "Glossary Term with this name already exists at this level of the Business Glossary");
            }
          }
        });
  }
}
