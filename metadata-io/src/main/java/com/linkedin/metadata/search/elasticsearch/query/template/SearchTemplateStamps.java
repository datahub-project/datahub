package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.search.template.SearchTemplateProperties;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Every accepted write sets {@code lastModified}. The first write sets {@code created}. */
public final class SearchTemplateStamps {
  private SearchTemplateStamps() {}

  public static void apply(
      @Nonnull BatchItem item,
      @Nonnull SearchTemplateProperties properties,
      @Nullable SearchTemplateProperties stored) {
    AuditStamp writeStamp = item.getAuditStamp();
    long now = writeStamp != null ? writeStamp.getTime() : System.currentTimeMillis();
    Urn actor =
        writeStamp != null && writeStamp.getActor() != null
            ? writeStamp.getActor()
            : UrnUtils.getUrn(SYSTEM_ACTOR);
    if (!properties.hasCreated() || properties.getCreated() == null) {
      if (stored != null && stored.hasCreated() && stored.getCreated() != null) {
        properties.setCreated(stored.getCreated());
      } else {
        properties.setCreated(new AuditStamp().setTime(now).setActor(actor));
      }
    }
    properties.setLastModified(new AuditStamp().setTime(now).setActor(actor));
    if (item instanceof MCPItem mcp
        && mcp.getMetadataChangeProposal() != null
        && mcp.getMetadataChangeProposal().getAspect() != null) {
      mcp.getMetadataChangeProposal().setAspect(GenericRecordUtils.serializeAspect(properties));
    }
  }
}
