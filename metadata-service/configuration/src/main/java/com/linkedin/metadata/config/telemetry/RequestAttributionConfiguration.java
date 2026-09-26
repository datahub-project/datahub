package com.linkedin.metadata.config.telemetry;

import lombok.Data;

/**
 * POJO for the {@code telemetry.requestAttribution} block in application.yaml.
 *
 * <p>Everything here is off by default. When {@link #enabled} is true, GMS attaches a small set of
 * {@code datahub.*} attributes to the request's root span so that an OpenTelemetry backend can
 * attribute OpenSearch and database load to a DataHub actor and API operation without joining child
 * spans. Nothing is written to logs or exposed in the UI.
 */
@Data
public class RequestAttributionConfiguration {
  /**
   * Master switch. Creates a per-request accumulator and stamps {@code datahub.es.*}, {@code
   * datahub.pg.*}, {@code datahub.request.*} attributes on the request span.
   */
  private boolean enabled = false;

  /**
   * Send an {@code X-Opaque-Id} header (trace id, actor urn, request id) on OpenSearch search-side
   * requests so OpenSearch slow logs, the tasks API and Query Insights can name the caller.
   */
  private boolean opensearchOpaqueId = false;

  /**
   * Prefix every SQL statement issued while a request is in scope with {@code
   * /*datahub_actor='<urn>',datahub_op='<operation>'*&#47;} so the database's statement log and
   * {@code pg_stat_activity} name the DataHub actor directly, with no join. Deliberately excludes
   * the trace id: the comment varies only by actor and operation, so the driver's
   * prepared-statement cache still converges. Writes actor urns into the database log; leave off
   * unless that is wanted.
   */
  private boolean postgresActorComment = false;

  /**
   * Emit a zero-length {@code datahub.request.start} span when a request begins, so a backend can
   * show in-flight requests before they complete.
   */
  private boolean startMarker = false;
}
