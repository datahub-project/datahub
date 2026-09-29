package com.linkedin.metadata.ingestion;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * The per-connector entries for a single GMS server release in the ingestion CLI version matrix.
 *
 * <p>Maps connector type (e.g. {@code "snowflake"}, {@code "bigquery"}) to that connector's {@link
 * ConnectorEntry}. The named lookup method {@link #getConnectorEntry(String)} makes the meaning of
 * the string key obvious at every call site.
 */
public final class ServerEntry {

  /** Empty entry returned to callers that ask about an unknown server version. */
  public static final ServerEntry EMPTY = new ServerEntry(Collections.emptyMap());

  private final Map<String, ConnectorEntry> connectorEntries;

  public ServerEntry(Map<String, ConnectorEntry> connectorEntries) {
    this.connectorEntries =
        connectorEntries == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(connectorEntries);
  }

  /**
   * Lookup the matrix entry for a connector type (e.g. {@code "snowflake"}). Returns {@code null}
   * when the connector has no entry under this server version — callers fall through to the next
   * tier of the resolution ladder.
   */
  public ConnectorEntry getConnectorEntry(String connectorType) {
    return connectorEntries.get(connectorType);
  }

  /** Connector types present under this server version. Useful for diagnostic logging. */
  public Set<String> getConnectorTypes() {
    return connectorEntries.keySet();
  }

  /** Number of connector entries for this server version. */
  public int size() {
    return connectorEntries.size();
  }
}
