/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.resolvers.template;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.DeletePageTemplateInput;
import com.linkedin.metadata.service.PageTemplateService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class DeletePageTemplateResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final PageTemplateService _pageTemplateService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final DeletePageTemplateInput input =
        bindArgument(environment.getArgument("input"), DeletePageTemplateInput.class);

    final String templateUrn = input.getUrn();
    final Urn urn = UrnUtils.getUrn(templateUrn);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            _pageTemplateService.deletePageTemplate(context.getOperationContext(), urn);
            log.info(String.format("Successfully deleted PageTemplate with urn %s", templateUrn));
            return true;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to delete PageTemplate with urn %s", templateUrn), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
