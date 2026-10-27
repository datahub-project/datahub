package com.linkedin.metadata.graph.inmemory;

import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.graph.lineage.core.NodeFacts;
import com.linkedin.metadata.search.LightningUrnFields;

/**
 * Node facts as GMS's lightning search reads them, computed once per node when a snapshot is built
 * so the lineage graph service never parses URNs: {@link LightningUrnFields#platform}, {@link
 * LightningUrnFields#environment} and, for a schema field, {@link LightningUrnFields#parent}.
 *
 * <p>The service reports a grouped schema field as its stored parent, so the parent must be the URN
 * {@link LightningUrnFields#schemaFieldReferenceUrn} reports. A node where that does not hold, or
 * whose facts cannot be read, is marked unreadable: a search that reaches it is left to GMS, as
 * before.
 */
public final class LightningNodeFacts implements NodeFacts {

  public static final LightningNodeFacts INSTANCE = new LightningNodeFacts();

  private LightningNodeFacts() {}

  @Override
  public Facts of(String urnString) {
    // The walk's entity type comes from the URN prefix, as the walk's rules match it.
    String type = URN_PARTS.of(urnString).entityType();
    try {
      Urn urn = UrnUtils.getUrn(urnString);
      String platform = LightningUrnFields.platform(type, urn);
      String environment = LightningUrnFields.environment(type, urn);
      if (!SCHEMA_FIELD_ENTITY_NAME.equals(type)) {
        return new Facts(type, platform, environment, null, false);
      }
      String parent = LightningUrnFields.parent(urn);
      Urn reported = LightningUrnFields.schemaFieldReferenceUrn(urn);
      boolean consistent = parent != null && reported != urn && parent.equals(reported.toString());
      return new Facts(type, platform, environment, consistent ? parent : null, !consistent);
    } catch (RuntimeException e) {
      return new Facts(type, null, null, null, true);
    }
  }
}
