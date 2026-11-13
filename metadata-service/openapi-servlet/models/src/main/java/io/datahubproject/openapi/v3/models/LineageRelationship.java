package io.datahubproject.openapi.v3.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.datahubproject.openapi.v2.models.GenericRelationship;
import javax.annotation.Nonnull;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;

/**
 * A lineage edge. In addition to the raw graph {@code source}/{@code destination} inherited from
 * {@link GenericRelationship}, it exposes which endpoint is {@code upstream} and which is {@code
 * downstream} — a distinction that depends on the relationship type's {@code isUpstream} flag, not
 * on how the edge is stored.
 */
@Data
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class LineageRelationship extends GenericRelationship {
  @Nonnull private String upstream;
  @Nonnull private String downstream;
}
