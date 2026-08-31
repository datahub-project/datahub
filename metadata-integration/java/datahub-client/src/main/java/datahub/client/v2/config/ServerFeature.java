package datahub.client.v2.config;

/**
 * Features that may be supported by a DataHub server.
 *
 * <p>Features can be detected through:
 *
 * <ul>
 *   <li>Config flags (e.g., patchCapable: true/false)
 *   <li>Version thresholds (e.g., OpenAPI requires Core >= 1.0.1 or Cloud >= 0.3.11)
 * </ul>
 */
public enum ServerFeature {
  /** Server supports JSON patch operations for metadata updates */
  PATCH_CAPABLE,

  /** Server supports stateful ingestion for incremental metadata updates */
  STATEFUL_INGESTION,

  /** Server supports impact analysis features */
  IMPACT_ANALYSIS,

  /** Server supports OpenAPI endpoints (alternative to RestLI) */
  OPENAPI_SDK,

  /** Server supports async API tracing for request tracking */
  API_TRACING,

  /** Server is running DataHub Cloud (vs self-hosted) */
  DATAHUB_CLOUD
}
