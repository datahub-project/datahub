package com.linkedin.metadata.graph.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.graph.write.GraphWriteSink;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Shared helpers for SqlSetup pgRouting graph rows (aligned with ES edge JSON shape). */
@Slf4j
public abstract class AbstractPostgresGraphWriteSink implements GraphWriteSink {

  protected static final ObjectMapper MAPPER = new ObjectMapper();

  @Nonnull private final String idHashAlgo;

  protected AbstractPostgresGraphWriteSink(@Nonnull String idHashAlgo) {
    this.idHashAlgo = idHashAlgo;
  }

  @Nonnull
  protected String idHashAlgo() {
    return idHashAlgo;
  }

  /**
   * Vertex BIGINT for {@code xxhash64_id} (or future columns) from the configured {@link
   * #idHashAlgo}.
   */
  protected long urnVertexId(@Nonnull Urn urn) {
    if ("XXHASH64".equalsIgnoreCase(idHashAlgo)) {
      return UrnFingerprint64.ofUtf8String(urn.toString());
    }
    throw new IllegalStateException("Unsupported postgres.pgGraph.idHashAlgo: " + idHashAlgo);
  }

  /**
   * Stable SMALLINT primary key for {@code edge_types.id} (see SqlSetup edge-type collision rules).
   */
  protected static short stableEdgeTypeCandidateId(@Nonnull String relationshipType) {
    int h = Hashing.murmur3_32(0).hashString(relationshipType, StandardCharsets.UTF_8).asInt();
    int positive = h & 0x7fff;
    return (short) (positive == 0 ? 1 : positive);
  }

  @Nonnull
  protected static String edgePropertiesJson(@Nonnull Edge edge) {
    Map<String, Object> props = edge.getProperties();
    LinkedHashMap<String, Object> merged = new LinkedHashMap<>();
    if (props != null && !props.isEmpty()) {
      merged.putAll(props);
    }
    if (edge.getVia() != null) {
      merged.put(Edge.EDGE_FIELD_VIA, edge.getVia().toString());
    }
    if (edge.getLifecycleOwner() != null) {
      merged.put(Edge.EDGE_FIELD_LIFECYCLE_OWNER, edge.getLifecycleOwner().toString());
    }
    if (merged.isEmpty()) {
      return "{}";
    }
    try {
      return MAPPER.writeValueAsString(merged);
    } catch (JsonProcessingException e) {
      log.warn("Failed to serialize edge properties; using empty object: {}", e.toString());
      return "{}";
    }
  }

  protected static long edgeTimestampMillis(@Nonnull Edge edge) {
    if (edge.getUpdatedOn() != null) {
      return edge.getUpdatedOn();
    }
    if (edge.getCreatedOn() != null) {
      return edge.getCreatedOn();
    }
    return System.currentTimeMillis();
  }
}
