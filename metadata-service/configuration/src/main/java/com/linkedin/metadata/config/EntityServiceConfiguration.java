package com.linkedin.metadata.config;

import javax.annotation.Nullable;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class EntityServiceConfiguration {
  private boolean alwaysEmitChangeLog = false;
  private boolean cdcModeChangeLog = false;
  @Nullable private Integer retry = null;
  private boolean enableBrowseV2 = false;
  private boolean postCommitRetentionEnabled = false;

  // Stamp emitModeMarker=sync (the marker the Python REST emitter's
  // respect_mcp_sync_marker reads, see Constants.EMIT_MODE_MARKER_KEY) onto
  // externally-originated sync writes (RESTLI/OPENAPI/GRAPHQL) so downstream
  // consumers (MCL -> platform events -> event actions) can preserve the sync
  // QoS of derived writes. Gated by DATAHUB_HONOR_SYNC_INGEST_FLAG.
  private boolean syncIngestStamping = false;
}
