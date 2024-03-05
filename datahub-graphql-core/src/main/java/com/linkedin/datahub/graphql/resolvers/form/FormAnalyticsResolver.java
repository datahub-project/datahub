package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.integration.IntegrationsService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FormAnalyticsResolver
    implements DataFetcher<CompletableFuture<FormAnalyticsResponse>> {

  private final EntityClient _entityClient;
  private final IntegrationsService _integrationsService;
  private final FeatureFlags _featureFlags;

  private static final String REPORTING_FORM_DATASET_NAME =
      Optional.ofNullable(System.getenv("FORMS_REPORTING_DATASET_NAME"))
          .orElse("reporting.forms.snapshot");

  public FormAnalyticsResolver(
      EntityClient entityClient,
      IntegrationsService integrationsService,
      FeatureFlags featureFlags) {
    _entityClient = entityClient;
    _integrationsService = integrationsService;
    _featureFlags = featureFlags;
  }

  public static String getReportingDatasetUrn() {
    return "urn:li:dataset:(urn:li:dataPlatform:datahub," + REPORTING_FORM_DATASET_NAME + ",PROD)";
  }

  @Override
  public CompletableFuture<FormAnalyticsResponse> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    final FormAnalyticsInput input =
        bindArgument(environment.getArgument("input"), FormAnalyticsInput.class);

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            FormAnalyticsResponse response = new FormAnalyticsResponse();
            if (!_featureFlags.isTaskCenterEnabled()) {
              response.setErrors(
                  List.of(
                      FormAnalyticsError.builder()
                          .setMessage(
                              "Feature flag is not enabled. Check with your admin to enable this feature.")
                          .setCode("403") // because the feature flag is not enabled
                          .build()));
              return response;
            }
            ArrayList<FormAnalyticsRow> lines = new ArrayList<FormAnalyticsRow>();
            ArrayList<FormAnalyticsError> errors = new ArrayList<FormAnalyticsError>();
            String datasetUrn = getReportingDatasetUrn();
            if (input.getQueryString() == null || input.getQueryString().isEmpty()) {
              response.setErrors(
                  List.of(
                      FormAnalyticsError.builder()
                          .setMessage("Query string is empty")
                          .setCode("400") // because the input is invalid
                          .build()));
              return response;
            }
            _integrationsService.query(
                datasetUrn,
                input.getQueryString(),
                response::setHeader,
                row -> {
                  lines.add(new FormAnalyticsRow(row, mapRowResults(row)));
                },
                error_messages -> {
                  for (String error : error_messages) {
                    errors.add(
                        FormAnalyticsError.builder()
                            .setMessage(error)
                            .setCode("502") // because our dependent service has returned an error
                            .build());
                  }
                });
            response.setTable(lines);
            if (!errors.isEmpty()) {
              response.setErrors(errors);
            }
            return response;
          } catch (Exception e) {
            FormAnalyticsResponse response = new FormAnalyticsResponse();
            response.setHeader(null);
            response.setTable(null);
            response.setErrors(
                List.of(
                    FormAnalyticsError.builder()
                        .setMessage("Failed to query analytics service due to :" + e)
                        .setCode("500") // because we have failed to process the request
                        .build()));
            log.error(String.format("Failed to perform update against input %s", input), e);
            return response;
          }
        });
  }

  private List<RowResult> mapRowResults(final List<String> row) {
    return row.stream()
        .map(
            rowEntry -> {
              RowResult result = new RowResult();
              result.setValue(rowEntry);
              try {
                final Urn urnValue = Urn.createFromString(rowEntry);
                result.setEntity(UrnToEntityMapper.map(urnValue));
              } catch (Exception e) {
                log.debug(String.format("Row entry is not an urn: %s", rowEntry));
              }
              return result;
            })
        .collect(Collectors.toList());
  }
}
