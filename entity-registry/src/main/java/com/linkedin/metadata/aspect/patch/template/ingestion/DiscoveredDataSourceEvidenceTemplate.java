package com.linkedin.metadata.aspect.patch.template.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceEvidence;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceEvidenceEntryMap;
import com.linkedin.metadata.aspect.patch.template.Template;
import javax.annotation.Nonnull;

/**
 * Evidence is a map keyed by "{detector}:{ingestion source urn}", so each ingestion run patches
 * only its own entries (e.g. {@code add /evidence/<key>}) and concurrent runs never overwrite each
 * other.
 */
public class DiscoveredDataSourceEvidenceTemplate
    implements Template<DiscoveredDataSourceEvidence> {

  @Override
  public DiscoveredDataSourceEvidence getSubtype(RecordTemplate recordTemplate)
      throws ClassCastException {
    if (recordTemplate instanceof DiscoveredDataSourceEvidence) {
      return (DiscoveredDataSourceEvidence) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to DiscoveredDataSourceEvidence");
  }

  @Override
  public Class<DiscoveredDataSourceEvidence> getTemplateType() {
    return DiscoveredDataSourceEvidence.class;
  }

  @Nonnull
  @Override
  public DiscoveredDataSourceEvidence getDefault() {
    return new DiscoveredDataSourceEvidence()
        .setEvidence(new DiscoveredDataSourceEvidenceEntryMap());
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    return baseNode;
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    return patched;
  }
}
