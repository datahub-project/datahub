package com.linkedin.metadata.entity.retention.buffer;

import java.io.Serializable;

/**
 * Coalescing key for the retention buffer: identifies a single retention request whose pending
 * writes should be collapsed to a single "keep max version" entry.
 *
 * <p>Interface (not a record) so an extension module can attach routing metadata without this
 * module knowing that metadata exists. OSS default: {@link SimpleRetentionKey}. Equality is the
 * implementation's responsibility — {@link SimpleRetentionKey#equals}/{@code hashCode} use {@code
 * (urn, aspectName)}; an extension module's key must include any routing metadata in
 * equals/hashCode so two requests that target different underlying databases do not coalesce into
 * one buffer entry.
 */
public interface RetentionKey extends Serializable {

  String urn();

  String aspectName();
}
