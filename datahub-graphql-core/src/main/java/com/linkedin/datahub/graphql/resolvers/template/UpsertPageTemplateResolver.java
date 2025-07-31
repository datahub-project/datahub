package com.linkedin.datahub.graphql.resolvers.template;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.DataHubPageTemplate;
import com.linkedin.datahub.graphql.generated.PageTemplateRowInput;
import com.linkedin.datahub.graphql.generated.PageTemplateScope;
import com.linkedin.datahub.graphql.generated.PageTemplateSurfaceType;
import com.linkedin.datahub.graphql.generated.UpsertPageTemplateInput;
import com.linkedin.datahub.graphql.types.template.PageTemplateMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.service.PageTemplateService;
import com.linkedin.template.DataHubPageTemplateRow;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class UpsertPageTemplateResolver
    implements DataFetcher<CompletableFuture<DataHubPageTemplate>> {

  private final PageTemplateService _pageTemplateService;

  @Override
  public CompletableFuture<DataHubPageTemplate> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final UpsertPageTemplateInput input =
        bindArgument(environment.getArgument("input"), UpsertPageTemplateInput.class);

    String urn = input.getUrn();
    List<PageTemplateRowInput> rows = input.getRows();
    PageTemplateScope scope = input.getScope();
    PageTemplateSurfaceType surfaceType = input.getSurfaceType();

    if (input.getScope().equals(PageTemplateScope.GLOBAL)
        && !AuthorizationUtils.canManageHomePageTemplates(context)) {
      throw new AuthorizationException("User does not have permission to update global templates.");
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final Urn templateUrn =
                _pageTemplateService.upsertPageTemplate(
                    context.getOperationContext(),
                    urn,
                    mapInputRows(rows),
                    com.linkedin.template.PageTemplateScope.valueOf(scope.toString()),
                    com.linkedin.template.PageTemplateSurfaceType.valueOf(surfaceType.toString()));

            EntityResponse response =
                _pageTemplateService.getPageTemplateEntityResponse(
                    context.getOperationContext(), templateUrn);
            return PageTemplateMapper.map(context, response);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    "Failed to perform upsert page template update against input %s", input),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  @Nonnull
  private List<DataHubPageTemplateRow> mapInputRows(List<PageTemplateRowInput> inputRows) {
    List<DataHubPageTemplateRow> finalRows = new ArrayList<>();
    inputRows.forEach(
        row -> {
          DataHubPageTemplateRow templateRow = new DataHubPageTemplateRow();
          UrnArray modules = new UrnArray();
          row.getModules().forEach(moduleUrn -> modules.add(UrnUtils.getUrn(moduleUrn)));
          templateRow.setModules(modules);
          finalRows.add(templateRow);
        });
    return finalRows;
  }
}
