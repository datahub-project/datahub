package com.linkedin.datahub.graphql.resolvers.form;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FormPrompt;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.datahub.graphql.generated.GlossaryTermsParams;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GlossaryTermsParamsResolver
    implements DataFetcher<CompletableFuture<GlossaryTermsParams>> {

  private final EntityClient _entityClient;

  public GlossaryTermsParamsResolver(@Nonnull final EntityClient entityClient) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
  }

  @Override
  public CompletableFuture<GlossaryTermsParams> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final FormPrompt prompt = environment.getSource();
    if (prompt.getGlossaryTermsParams() == null) {
      return null;
    }

    // first, set glossaryTermsParams with what we already know
    final GlossaryTermsParams glossaryTermsParams = new GlossaryTermsParams();
    glossaryTermsParams.setCardinality(prompt.getGlossaryTermsParams().getCardinality());
    glossaryTermsParams.setAllowedTerms(prompt.getGlossaryTermsParams().getAllowedTerms());
    glossaryTermsParams.setAllowedTermGroups(
        prompt.getGlossaryTermsParams().getAllowedTermGroups());
    final List<GlossaryTerm> resolvedAllowedTerms =
        prompt.getGlossaryTermsParams().getAllowedTerms() != null
            ? prompt.getGlossaryTermsParams().getAllowedTerms()
            : new ArrayList<>();
    glossaryTermsParams.setResolvedAllowedTerms(resolvedAllowedTerms);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (prompt.getGlossaryTermsParams().getAllowedTermGroups() == null) {
            return glossaryTermsParams;
          }

          // next, get glossary terms inside the specified allowedTermGroups
          List<Urn> termGroupUrns =
              prompt.getGlossaryTermsParams().getAllowedTermGroups().stream()
                  .map(node -> UrnUtils.getUrn(node.getUrn()))
                  .collect(Collectors.toList());
          Filter urnsFilter = ResolverUtils.createUrnFilter("parentNode", termGroupUrns);
          try {
            SearchResult result =
                _entityClient.search(
                    context.getOperationContext(),
                    Constants.GLOSSARY_TERM_ENTITY_NAME,
                    "*",
                    urnsFilter,
                    null,
                    0,
                    500);
            result
                .getEntities()
                .forEach(
                    resultEntity -> {
                      GlossaryTerm resultTerm = new GlossaryTerm();
                      resultTerm.setUrn(resultEntity.getEntity().toString());
                      resultTerm.setType(EntityType.GLOSSARY_TERM);
                      resolvedAllowedTerms.add(resultTerm);
                    });
            glossaryTermsParams.setResolvedAllowedTerms(resolvedAllowedTerms);
            return glossaryTermsParams;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    "Failed to search for terms inside of term groups for prompt %s",
                    prompt.getId()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
