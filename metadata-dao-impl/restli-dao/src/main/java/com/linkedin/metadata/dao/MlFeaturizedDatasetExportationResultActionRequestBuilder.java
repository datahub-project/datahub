package com.linkedin.metadata.dao;

import com.linkedin.common.urn.MlFeaturizedDatasetExportationResultUrn;
import com.linkedin.metadata.snapshot.MlFeaturizedDatasetExportationResultSnapshot;


/**
 * An action request builder for MlFeaturizedDatasetExportationResult entities.
 */
public class MlFeaturizedDatasetExportationResultActionRequestBuilder extends BaseActionRequestBuilder<MlFeaturizedDatasetExportationResultSnapshot,
    MlFeaturizedDatasetExportationResultUrn> {

  private static final String BASE_URI_TEMPLATE = "mlFeaturizedDatasetExportationResults";

  public MlFeaturizedDatasetExportationResultActionRequestBuilder() {
    super(MlFeaturizedDatasetExportationResultSnapshot.class, MlFeaturizedDatasetExportationResultUrn.class, BASE_URI_TEMPLATE);
  }
}