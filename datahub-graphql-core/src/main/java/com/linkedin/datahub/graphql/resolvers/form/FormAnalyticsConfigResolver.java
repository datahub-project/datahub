package com.linkedin.datahub.graphql.resolvers.form;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.FormAnalyticsConfig;
import com.linkedin.datahub.graphql.generated.FormAnalyticsError;
import com.linkedin.metadata.integration.IntegrationsService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FormAnalyticsConfigResolver
    implements DataFetcher<CompletableFuture<FormAnalyticsConfig>> {
  private static final String FEATURE_FLAG_PREFIX = "FEATURE_DOCUMENTATION_FORMS_";

  private static final Optional<String> REPORTING_DATASET_BUCKET_PREFIX =
      Optional.ofNullable(System.getenv(FEATURE_FLAG_PREFIX + "REPORTING_DATASET_BUCKET_PREFIX"));
  private static final String REPORTING_FORM_DATASET_NAME =
      Optional.ofNullable(System.getenv(FEATURE_FLAG_PREFIX + "REPORTING_DATASET_NAME"))
          .orElse("reporting.forms.snapshot");

  private static final String REPORTING_PLATFORM_NAME =
      Optional.ofNullable(System.getenv(FEATURE_FLAG_PREFIX + "REPORTING_PLATFORM_NAME"))
          .orElse("acryl");

  public static String getReportingDatasetUrn() {
    return "urn:li:dataset:(urn:li:dataPlatform:"
        + REPORTING_PLATFORM_NAME
        + ","
        + REPORTING_FORM_DATASET_NAME
        + ",PROD)";
  }

  public static Optional<String> getReportingBucketPrefix() {
    return REPORTING_DATASET_BUCKET_PREFIX;
  }

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

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            FormAnalyticsConfig response = new FormAnalyticsConfig();
            // Sharing the same feature flag as task center
            response.setEnabled(_featureFlags.isDocumentationFormsEnabled());
            if (!response.getEnabled()) {
              // If the feature flag is not enabled, we should not proceed with the request
              return response;
            }
            String datasetUrn = getReportingDatasetUrn();
            response.setDatasetUrn(datasetUrn);

            Optional<String> bucketPrefix = getReportingBucketPrefix();
            bucketPrefix.ifPresent(response::setPhysicalUriPrefix);
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
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
