package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.form.FormAnalyticsConfigResolver.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.metadata.integration.ResourceNotFoundException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FormAnalyticsResolver
    implements DataFetcher<CompletableFuture<FormAnalyticsResponse>> {

  private final EntityClient _entityClient;
  private final IntegrationsService _integrationsService;
  private final FeatureFlags _featureFlags;

  public FormAnalyticsResolver(
      EntityClient entityClient,
      IntegrationsService integrationsService,
      FeatureFlags featureFlags) {
    _entityClient = entityClient;
    _integrationsService = integrationsService;
    _featureFlags = featureFlags;
  }

  @Override
  public CompletableFuture<FormAnalyticsResponse> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    final FormAnalyticsInput input =
        bindArgument(environment.getArgument("input"), FormAnalyticsInput.class);
    final FormAnalyticsFlags flags = input.getFormAnalyticsFlags();
    List<FormAnalyticsRow> lines = new CopyOnWriteArrayList<>();
    List<FormAnalyticsError> errors = new CopyOnWriteArrayList<>();
    String datasetUrn = getReportingDatasetUrn();
    FormAnalyticsResponse response = new FormAnalyticsResponse();

    return _integrationsService
        .query(
            datasetUrn,
            input.getQueryString(),
            response::setHeader,
            row -> {
              lines.add(new FormAnalyticsRow(mapRowResults(context, row, flags)));
            },
            error_messages -> {
              for (String error : error_messages) {
                errors.add(
                    FormAnalyticsError.builder()
                        .setMessage(error)
                        .setCode("502") // because our dependent service has returned an error
                        .build());
              }
            })
        .thenCompose(
            queryCompleted ->
                GraphQLConcurrencyUtils.supplyAsync(
                    () -> {
                      try {
                        if (!_featureFlags.isDocumentationFormsEnabled()) {
                          response.setErrors(
                              List.of(
                                  FormAnalyticsError.builder()
                                      .setMessage(
                                          "Feature flag is not enabled. Check with your admin to enable this feature.")
                                      .setCode("403") // because the feature flag is not enabled
                                      .build()));
                          return response;
                        }

                        if (input.getQueryString() == null || input.getQueryString().isEmpty()) {
                          response.setErrors(
                              List.of(
                                  FormAnalyticsError.builder()
                                      .setMessage("Query string is empty")
                                      .setCode("400") // because the input is invalid
                                      .build()));
                          return response;
                        }
                        response.setTable(lines);
                        if (!errors.isEmpty()) {
                          response.setErrors(errors);
                        }
                        return response;
                      } catch (Exception e) {
                        response.setHeader(null);
                        response.setTable(null);
                        String code = (e instanceof ResourceNotFoundException) ? "404" : "500";
                        response.setErrors(
                            List.of(
                                FormAnalyticsError.builder()
                                    .setMessage("Failed to query analytics service due to: " + e)
                                    .setCode(code) // because we have failed to process the request
                                    .build()));
                        log.error(
                            String.format("Failed to perform update against input %s", input), e);
                        return response;
                      }
                    },
                    this.getClass().getSimpleName(),
                    "get"));
  }

  private List<RowResult> mapRowResults(
      @Nullable final QueryContext context,
      final List<String> row,
      @Nullable final FormAnalyticsFlags flags) {
    return row.stream()
        .map(
            rowEntry -> {
              RowResult result = new RowResult();
              result.setValue(rowEntry);
              try {
                final Urn urnValue = Urn.createFromString(rowEntry);
                final boolean skipHydration =
                    flags != null && flags.getSkipAssetHydration().equals(true);
                if (!skipHydration
                    && _entityClient.exists(context.getOperationContext(), urnValue)) {
                  result.setEntity(UrnToEntityMapper.map(context, urnValue));
                }
              } catch (Exception e) {
                log.debug(String.format("Row entry is not an urn: %s", rowEntry));
              }
              return result;
            })
        .collect(Collectors.toList());
  }
}
