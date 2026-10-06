package com.linkedin.metadata.ingestion;

/**
 * Why a matrix refresh failed, so an operator reading GMS logs can tell an access problem apart
 * from a bad document apart from a transient outage — three failures that look identical in a stack
 * trace but need completely different fixes.
 *
 * <p>Every {@link IngestionCliVersionMatrixSource} prefixes its failure WARN with {@link #token()}
 * in brackets, so {@code grep '\[permission\]'} over GMS logs finds exactly the refreshes that need
 * a credentials or bucket-policy fix, and a log-based alert can route on the token rather than on
 * message wording that changes.
 */
public enum MatrixRefreshFailure {

  /**
   * Credentials, IAM policy, bucket policy, or auth token rejected the read.
   *
   * <p>The hint names the mechanism rather than a service account, because the identity GMS runs as
   * is deployment-specific: the Helm chart defaults {@code global.serviceAccount.name} to the
   * release's fullname, and deployments routinely override it.
   */
  PERMISSION(
      "permission",
      "grant the service account GMS runs as (GKE Workload Identity / EKS IRSA) read access to the "
          + "matrix object, or set authToken for http"),

  /** The URI addresses an object, bucket, or path that does not exist. */
  NOT_FOUND("not-found", "check ingestion.cliVersionMatrix.uri points at an existing object"),

  /** The document was read but is not valid JSON, or violates the matrix schema. */
  PAYLOAD("payload", "fix the matrix document"),

  /** Network, timeout, or otherwise unclassified — usually transient and self-healing. */
  TRANSPORT("transport", "usually transient; check connectivity to the matrix location"),
  ;

  private final String token;
  private final String hint;

  MatrixRefreshFailure(String token, String hint) {
    this.token = token;
    this.hint = hint;
  }

  /** Stable, greppable identifier logged in brackets. Do not reword — alerts may match on it. */
  public String token() {
    return token;
  }

  /** Short operator-facing next step, appended to the failure WARN. */
  public String hint() {
    return hint;
  }

  /**
   * Classifies an HTTP (or HTTP-shaped, e.g. S3/GCS service) status code. Anything that is not
   * clearly an authorization or existence problem is treated as transport, since a 5xx or a 429 is
   * retried on the next refresh tick.
   */
  public static MatrixRefreshFailure forHttpStatus(int statusCode) {
    return switch (statusCode) {
      case 401, 403 -> PERMISSION;
      case 404, 410 -> NOT_FOUND;
      default -> TRANSPORT;
    };
  }
}
