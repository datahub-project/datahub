package com.linkedin.datahub.graphql.resolvers.glossary;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.persistAspect;

import com.linkedin.common.GlossaryTermUrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.RelatedTermsInput;
import com.linkedin.datahub.graphql.generated.TermRelationshipType;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryRelatedTerms;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class RemoveRelatedTermsResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService<?> _entityService;
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final RelatedTermsInput input =
        bindArgument(environment.getArgument("input"), RelatedTermsInput.class);
    final Urn urn = Urn.createFromString(input.getUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (GlossaryUtils.canUpdateGlossaryEntity(urn, context, _entityClient)) {
            try {
              final TermRelationshipType relationshipType = input.getRelationshipType();
              final List<Urn> termUrnsToRemove =
                  input.getTermUrns().stream().map(UrnUtils::getUrn).collect(Collectors.toList());

              if (!urn.getEntityType().equals(Constants.GLOSSARY_TERM_ENTITY_NAME)
                  || !_entityService.exists(context.getOperationContext(), urn, true)) {
                throw new IllegalArgumentException(
                    String.format(
                        "Failed to update %s. %s either does not exist or is not a glossaryTerm.",
                        urn, urn));
              }

              Urn actor = Urn.createFromString(((QueryContext) context).getActorUrn());

              GlossaryRelatedTerms glossaryRelatedTerms =
                  (GlossaryRelatedTerms)
                      EntityUtils.getAspectFromEntity(
                          context.getOperationContext(),
                          urn.toString(),
                          Constants.GLOSSARY_RELATED_TERM_ASPECT_NAME,
                          _entityService,
                          null);
              if (glossaryRelatedTerms == null) {
                throw new RuntimeException(
                    String.format("Related Terms for this Urn do not exist: %s", urn));
              }

              if (relationshipType == TermRelationshipType.isA) {
                if (!glossaryRelatedTerms.hasIsRelatedTerms()) {
                  throw new RuntimeException(
                      "Failed to remove from GlossaryRelatedTerms as they do not exist for this Glossary Term");
                }
                final GlossaryTermUrnArray existingTermUrns =
                    glossaryRelatedTerms.getIsRelatedTerms();

                existingTermUrns.removeIf(
                    termUrn -> termUrnsToRemove.stream().anyMatch(termUrn::equals));
                persistAspect(
                    context.getOperationContext(),
                    urn,
                    Constants.GLOSSARY_RELATED_TERM_ASPECT_NAME,
                    glossaryRelatedTerms,
                    actor,
                    _entityService);
                return true;
              } else {
                if (!glossaryRelatedTerms.hasHasRelatedTerms()) {
                  throw new RuntimeException(
                      "Failed to remove from GlossaryRelatedTerms as they do not exist for this Glossary Term");
                }
                final GlossaryTermUrnArray existingTermUrns =
                    glossaryRelatedTerms.getHasRelatedTerms();

                existingTermUrns.removeIf(
                    termUrn -> termUrnsToRemove.stream().anyMatch(termUrn::equals));
                persistAspect(
                    context.getOperationContext(),
                    urn,
                    Constants.GLOSSARY_RELATED_TERM_ASPECT_NAME,
                    glossaryRelatedTerms,
                    actor,
                    _entityService);
                return true;
              }
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Failed to removes related terms from %s", input.getUrn()), e);
            }
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
