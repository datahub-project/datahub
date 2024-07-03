package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.getEntityNames;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Form;
import com.linkedin.datahub.graphql.generated.FormFilter;
import com.linkedin.datahub.graphql.generated.FormForActor;
import com.linkedin.datahub.graphql.resolvers.search.SearchUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.form.FormInfo;
import com.linkedin.form.FormType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.FormService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NumEntitiesToCompleteResolver implements DataFetcher<CompletableFuture<Integer>> {

  private final EntityClient _entityClient;
  private final FormService _formService;

  public NumEntitiesToCompleteResolver(
      @Nonnull final EntityClient entityClient, @Nonnull final FormService formService) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
    _formService = Objects.requireNonNull(formService, "formService must not be null");
  }

  @Override
  public CompletableFuture<Integer> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final Authentication authentication = context.getAuthentication();
    final SearchFlags searchFlags = new SearchFlags();
    // always skip cache on this resolver since the number of entities to complete often updates
    searchFlags.setSkipCache(true);
    final Urn userUrn = UrnUtils.getUrn(context.getActorUrn());
    final FormForActor formForActor = environment.getSource();
    final Form form = formForActor.getForm();
    final Urn formUrn = UrnUtils.getUrn(form.getUrn());

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            // 1. fetch the formInfo aspect from database
            FormInfo formInfo = _formService.getFormInfo(context.getOperationContext(), formUrn);

            // 2. generate the form filter to filter entities based on form criteria for completion
            final FormFilter formFilter = createFormFilter(formInfo, formUrn, userUrn);
            final Filter filter =
                SearchUtils.getFormFilter(context.getOperationContext(), formFilter, _formService);

            SearchResult searchResult =
                _entityClient.searchAcrossEntities(
                    context.getOperationContext().withSearchFlags(flags -> searchFlags),
                    getEntityNames(null),
                    "*",
                    filter,
                    0,
                    0,
                    null,
                    null);

            // 3. return the number of entities that match this query
            return searchResult.getNumEntities();
          } catch (Exception e) {
            log.error(
                String.format(
                    "Failed to determine the number of entities to complete for form with urn %s. Returning 0.",
                    form.getUrn()),
                e);
          }
          return 0;
        });
  }

  private FormInfo getFormInfoFromResponse(
      @Nullable final EntityResponse response, @Nonnull final Urn formUrn) {
    if (response != null && response.getAspects().containsKey(Constants.FORM_INFO_ASPECT_NAME)) {
      return new FormInfo(
          response.getAspects().get(Constants.FORM_INFO_ASPECT_NAME).getValue().data());
    }
    throw new RuntimeException(
        String.format("Form with urn %s does not have a formInfo aspect", formUrn));
  }

  private FormFilter createFormFilter(
      @Nonnull final FormInfo formInfo, @Nonnull final Urn formUrn, @Nonnull final Urn userUrn) {
    final FormFilter formFilter = new FormFilter();
    formFilter.setFormUrn(formUrn.toString());
    formFilter.setAssignedActor(userUrn.toString());
    if (formInfo.getType().equals(FormType.VERIFICATION)) {
      formFilter.setIsFormVerified(false);
    } else {
      formFilter.setIsFormComplete(false);
    }
    return formFilter;
  }
}
