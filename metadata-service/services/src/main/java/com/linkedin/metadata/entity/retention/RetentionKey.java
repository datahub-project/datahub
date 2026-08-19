package com.linkedin.metadata.entity.retention;

import java.io.Serializable;
import javax.annotation.Nonnull;

/**
 * Coalescing key for the retention buffer: identifies a single retention request whose pending
 * writes should be collapsed to a single "keep max version" entry.
 *
 * <p>Interface (not a record) so an extension module can attach routing metadata without this
 * module knowing that metadata exists. OSS default: {@code SimpleRetentionKey}. Equality is the
 * implementation's responsibility — {@code SimpleRetentionKey#equals}/{@code hashCode} use {@code
 * (urn, aspectName)}; an extension module's key must include any routing metadata in
 * equals/hashCode so two requests that target different underlying databases do not coalesce into
 * one buffer entry.
 *
 * <p>Implementors MUST declare {@code serialVersionUID} explicitly (e.g. {@code private static
 * final long serialVersionUID = 1L;}). Auto-generated UIDs change on recompilation and break
 * Hazelcast IMap entries on rolling deploys.
 */
public interface RetentionKey extends Serializable {

  @Nonnull
  String urn();

  @Nonnull
  String aspectName();
}
