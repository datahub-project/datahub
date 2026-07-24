package com.linkedin.metadata.config;

import lombok.Data;

/**
 * S3-fetched ingestion CLI version matrix configuration. Bound under {@code
 * ingestion.cliVersionMatrix.s3} in application.yaml. Selected when {@code
 * ingestion.cliVersionMatrix.source} is {@code "s3"}.
 *
 * <p>Reads the matrix object from a (typically private) S3 bucket using GMS's ambient AWS
 * credentials — the pod's IAM role, resolved by the shared {@code s3Util} bean. Unlike the HTTP
 * source, the bucket needs no public-read or static token: each request is SigV4-signed, so the
 * bucket can stay private and be shared cross-account via bucket policy. Region/credentials are
 * taken from the {@code s3Util} bean's configuration ({@code AWS_REGION} / {@code
 * datahub.s3.roleArn}), so they are not repeated here.
 */
@Data
public class S3MatrixSourceConfiguration {

  /**
   * S3 bucket holding the matrix JSON object. When empty, the factory wires a no-op matrix source.
   */
  private String bucket;

  /** Object key of the matrix JSON within the bucket (e.g. {@code matrix.json}). */
  private String key;

  /**
   * How often (in seconds) to re-fetch the matrix. The 600s (10 minute) default is supplied by
   * application.yaml; the field itself has no default (a bare {@code int} is 0), and the factory
   * treats a non-positive value as a misconfiguration and degrades to a no-op source.
   */
  private int refreshSeconds;
}
