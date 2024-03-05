package com.linkedin.datahub.graphql.resolvers.form;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.FormAnalyticsConfig;
import com.linkedin.datahub.graphql.generated.FormAnalyticsError;
import com.linkedin.metadata.integration.IntegrationsService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FormAnalyticsConfigResolver
    implements DataFetcher<CompletableFuture<FormAnalyticsConfig>> {
  private final IntegrationsService _integrationsService;
  private final FeatureFlags _featureFlags;

  public FormAnalyticsConfigResolver(
      IntegrationsService integrationsService, FeatureFlags featureFlags) {
    _integrationsService = integrationsService;
    _featureFlags = featureFlags;
  }

  @Override
  public CompletableFuture<FormAnalyticsConfig> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            FormAnalyticsConfig response = new FormAnalyticsConfig();
            // Sharing the same feature flag as task center
            response.setEnabled(_featureFlags.isDocumentationFormsEnabled());
            if (!response.getEnabled()) {
              // If the feature flag is not enabled, we should not proceed with the request
              return response;
            }
            String datasetUrn = FormAnalyticsResolver.getReportingDatasetUrn();
            response.setDatasetUrn(datasetUrn);

            // TODO: get dataset uri
            return response;
          } catch (Exception e) {
            FormAnalyticsConfig response = new FormAnalyticsConfig();
            response.setErrors(
                List.of(
                    FormAnalyticsError.builder()
                        .setMessage("Failed to query analytics service due to :" + e)
                        .setCode("500") // because we have failed to process the request
                        .build()));
            log.error("Failed to retrieve config", e);
            return response;
          }
        });
  }
}
