package com.linkedin.metadata.aspect.patch.template.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceDecision;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceDecisionEventArray;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceStatus;
import com.linkedin.metadata.aspect.patch.template.Template;
import javax.annotation.Nonnull;

/**
 * Lets a writer set the status and append to history in one PATCH ({@code add /history/-}), so a
 * transition and its audit event are applied together against the latest stored decision.
 */
public class DiscoveredDataSourceDecisionTemplate
    implements Template<DiscoveredDataSourceDecision> {

  @Override
  public DiscoveredDataSourceDecision getSubtype(RecordTemplate recordTemplate)
      throws ClassCastException {
    if (recordTemplate instanceof DiscoveredDataSourceDecision) {
      return (DiscoveredDataSourceDecision) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to DiscoveredDataSourceDecision");
  }

  @Override
  public Class<DiscoveredDataSourceDecision> getTemplateType() {
    return DiscoveredDataSourceDecision.class;
  }

  @Nonnull
  @Override
  public DiscoveredDataSourceDecision getDefault() {
    return new DiscoveredDataSourceDecision()
        .setStatus(DiscoveredDataSourceStatus.NEW)
        .setHistory(new DiscoveredDataSourceDecisionEventArray());
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
