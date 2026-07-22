package com.linkedin.metadata.config;

import lombok.Data;

/**
 * GCS-fetched ingestion CLI version matrix configuration. Bound under {@code
 * ingestion.cliVersionMatrix.gcs} in application.yaml. Selected when {@code
 * ingestion.cliVersionMatrix.source} is {@code "gcs"}.
 *
 * <p>Reads the matrix object from a (typically private) GCS bucket using GMS's ambient GCP
 * credentials — Workload Identity Federation on GKE, resolved by the shared {@code gcsUtil} bean.
 * Project/credentials are taken from the pod's ambient environment (Application Default
 * Credentials), so they are not repeated here.
 */
@Data
public class GcsMatrixSourceConfiguration {

  /**
   * GCS bucket holding the matrix JSON object. When empty, the factory wires a no-op matrix source.
   */
  private String bucket;

  /** Object name of the matrix JSON within the bucket (e.g. {@code matrix.json}). */
  private String key;

  /**
   * How often (in seconds) to re-fetch the matrix. The 600s (10 minute) default is supplied by
   * application.yaml; the field itself has no default (a bare {@code int} is 0), and the factory
   * treats a non-positive value as a misconfiguration and degrades to a no-op source.
   */
  private int refreshSeconds;
}
