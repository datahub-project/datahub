package com.linkedin.datahub.graphql.resolvers.glossary;

import com.linkedin.common.GlossaryTermUrnArray;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.RelatedTermsInput;
import com.linkedin.datahub.graphql.generated.TermRelationshipType;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.glossary.GlossaryRelatedTerms;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;

@Slf4j
@RequiredArgsConstructor
public class AddRelatedTermsResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService _entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final RelatedTermsInput input = bindArgument(environment.getArgument("input"), RelatedTermsInput.class);

    return CompletableFuture.supplyAsync(() -> {
      if (GlossaryUtils.canManageGlossaries(context)) {
        try {
          final TermRelationshipType relationshipType = input.getRelationshipType();
          final Urn urn = Urn.createFromString(input.getUrn());
          final List<Urn> termUrns = input.getTermUrns().stream()
              .map(UrnUtils::getUrn)
              .collect(Collectors.toList());
          validateRelatedTermsInput(urn, termUrns);
          Urn actor = Urn.createFromString(((QueryContext) context).getActorUrn());

          GlossaryRelatedTerms glossaryRelatedTerms = (GlossaryRelatedTerms) getAspectFromEntity(
              urn.toString(),
              Constants.GLOSSARY_RELATED_TERM_ASPECT_NAME,
              _entityService,
              null
          );
          if (glossaryRelatedTerms == null) {
            glossaryRelatedTerms = new GlossaryRelatedTerms();
          }

          if (relationshipType == TermRelationshipType.isA) {
            if (!glossaryRelatedTerms.hasIsRelatedTerms()) {
              glossaryRelatedTerms.setIsRelatedTerms(new GlossaryTermUrnArray());
            }
            final GlossaryTermUrnArray existingTermUrns = glossaryRelatedTerms.getIsRelatedTerms();

            return updateRelatedTerms(termUrns, existingTermUrns, urn, glossaryRelatedTerms, actor);
          } else {
            if (!glossaryRelatedTerms.hasHasRelatedTerms()) {
              glossaryRelatedTerms.setHasRelatedTerms(new GlossaryTermUrnArray());
            }
            final GlossaryTermUrnArray existingTermUrns = glossaryRelatedTerms.getHasRelatedTerms();

            return updateRelatedTerms(termUrns, existingTermUrns, urn, glossaryRelatedTerms, actor);
          }
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to add related terms to %s", input.getUrn()), e);
        }
      }
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }

  public Boolean validateRelatedTermsInput(Urn urn, List<Urn> termUrns) {
    if (!urn.getEntityType().equals(Constants.GLOSSARY_TERM_ENTITY_NAME) || !_entityService.exists(urn)) {
      throw new IllegalArgumentException(String.format("Failed to update %s. %s either does not exist or is not a glossaryTerm.", urn, urn));
    }

    for (Urn termUrn : termUrns) {
      if (termUrn.equals(urn)) {
        throw new IllegalArgumentException(String.format("Failed to update %s. Tried to create related term with itself.", urn));
      } else if (!termUrn.getEntityType().equals(Constants.GLOSSARY_TERM_ENTITY_NAME)) {
        throw new IllegalArgumentException(String.format("Failed to update %s. %s is not a glossaryTerm.", urn, termUrn));
      } else if (!_entityService.exists(termUrn)) {
        throw new IllegalArgumentException(String.format("Failed to update %s. %s does not exist.", urn, termUrn));
      }
    }
    return true;
  }

  private Boolean updateRelatedTerms(List<Urn> termUrns, GlossaryTermUrnArray existingTermUrns, Urn urn, GlossaryRelatedTerms glossaryRelatedTerms, Urn actor) {
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
    persistAspect(urn, Constants.GLOSSARY_RELATED_TERM_ASPECT_NAME, glossaryRelatedTerms, actor, _entityService);
    return true;
  }
}
