package io.datahubproject.openlineage.customfacet;

import io.datahubproject.openlineage.customfacet.CompatibilityFacetCatalog.AttachmentPoint;
import io.datahubproject.openlineage.customfacet.CompatibilityFacetCatalog.SupportStatus;
import java.net.URI;
import java.util.Set;

public record CompatibilityFacetContract(
    AttachmentPoint attachment,
    String key,
    SupportStatus status,
    Set<URI> acceptedSchemaUrls,
    Set<ProducerUriPattern> acceptedProducerPatterns) {
  public CompatibilityFacetContract {
    acceptedSchemaUrls = Set.copyOf(acceptedSchemaUrls);
    acceptedProducerPatterns = Set.copyOf(acceptedProducerPatterns);
  }

  public boolean matches(AttachmentPoint candidateAttachment, URI schemaUrl, URI producer) {
    return attachment == candidateAttachment
        && acceptedSchemaUrls.contains(schemaUrl)
        && acceptedProducerPatterns.stream().anyMatch(pattern -> pattern.matches(producer));
  }
}
