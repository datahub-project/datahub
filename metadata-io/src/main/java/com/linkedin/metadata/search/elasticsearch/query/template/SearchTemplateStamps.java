package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.search.template.SearchTemplateProperties;
import javax.annotation.Nonnull;

/**
 * A caller who omits {@code apiVersion} stores contract version 1. Write time and actor stay on
 * system metadata.
 */
public final class SearchTemplateStamps {
  private SearchTemplateStamps() {}

  public static boolean apply(
      @Nonnull BatchItem item, @Nonnull SearchTemplateProperties properties) {
    if (properties.hasApiVersion() && properties.getApiVersion() != null) {
      return false;
    }
    SearchTemplateCapabilities.stampApiVersion(properties);
    if (item instanceof MCPItem mcp
        && mcp.getMetadataChangeProposal() != null
        && mcp.getMetadataChangeProposal().getAspect() != null) {
      mcp.getMetadataChangeProposal().setAspect(GenericRecordUtils.serializeAspect(properties));
    }
    return true;
  }
}
