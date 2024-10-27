package com.linkedin.datahub.graphql.resolvers.glossary;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;

import com.linkedin.common.GlossaryTermUrnArray;
import com.linkedin.common.urn.GlossaryTermUrn;
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
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class AddRelatedTermsResolver implements DataFetcher<CompletableFuture<Boolean>> {

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
              final List<Urn> termUrns =
                  input.getTermUrns().stream().map(UrnUtils::getUrn).collect(Collectors.toList());
              validateRelatedTermsInput(context.getOperationContext(), urn, termUrns);
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
                glossaryRelatedTerms = new GlossaryRelatedTerms();
              }

              if (relationshipType == TermRelationshipType.isA) {
                if (!glossaryRelatedTerms.hasIsRelatedTerms()) {
                  glossaryRelatedTerms.setIsRelatedTerms(new GlossaryTermUrnArray());
                }
                final GlossaryTermUrnArray existingTermUrns =
                    glossaryRelatedTerms.getIsRelatedTerms();

                return updateRelatedTerms(
                    context.getOperationContext(),
                    termUrns,
                    existingTermUrns,
                    urn,
                    glossaryRelatedTerms,
                    actor);
              } else {
                if (!glossaryRelatedTerms.hasHasRelatedTerms()) {
                  glossaryRelatedTerms.setHasRelatedTerms(new GlossaryTermUrnArray());
                }
                final GlossaryTermUrnArray existingTermUrns =
                    glossaryRelatedTerms.getHasRelatedTerms();

                return updateRelatedTerms(
                    context.getOperationContext(),
                    termUrns,
                    existingTermUrns,
                    urn,
                    glossaryRelatedTerms,
                    actor);
              }
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Failed to add related terms to %s", input.getUrn()), e);
            }
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        },
        this.getClass().getSimpleName(),
        "get");
  }

  public Boolean validateRelatedTermsInput(
      @Nonnull OperationContext opContext, Urn urn, List<Urn> termUrns) {
    if (!urn.getEntityType().equals(Constants.GLOSSARY_TERM_ENTITY_NAME)
        || !_entityService.exists(opContext, urn, true)) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to update %s. %s either does not exist or is not a glossaryTerm.", urn, urn));
    }

    for (Urn termUrn : termUrns) {
      if (termUrn.equals(urn)) {
        throw new IllegalArgumentException(
            String.format("Failed to update %s. Tried to create related term with itself.", urn));
      } else if (!termUrn.getEntityType().equals(Constants.GLOSSARY_TERM_ENTITY_NAME)) {
        throw new IllegalArgumentException(
            String.format("Failed to update %s. %s is not a glossaryTerm.", urn, termUrn));
      } else if (!_entityService.exists(opContext, termUrn, true)) {
        throw new IllegalArgumentException(
            String.format("Failed to update %s. %s does not exist.", urn, termUrn));
      }
    }
    return true;
  }

  private Boolean updateRelatedTerms(
      @Nonnull OperationContext opContext,
      List<Urn> termUrns,
      GlossaryTermUrnArray existingTermUrns,
      Urn urn,
      GlossaryRelatedTerms glossaryRelatedTerms,
      Urn actor) {
    List<Urn> termsToAdd = new ArrayList<>();
    for (Urn termUrn : termUrns) {
      if (existingTermUrns.stream().anyMatch(association -> association.equals(termUrn))) {
        continue;
      }
      termsToAdd.add(termUrn);
    }

    if (termsToAdd.size() == 0) {
      return true;
    }

    for (Urn termUrn : termsToAdd) {
      GlossaryTermUrn newUrn = new GlossaryTermUrn(termUrn.getId());

      existingTermUrns.add(newUrn);
    }
    persistAspect(
        opContext,
        urn,
        Constants.GLOSSARY_RELATED_TERM_ASPECT_NAME,
        glossaryRelatedTerms,
        actor,
        _entityService);
    return true;
  }
}
