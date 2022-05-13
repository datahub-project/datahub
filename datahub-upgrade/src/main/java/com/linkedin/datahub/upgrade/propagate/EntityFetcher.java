package com.linkedin.datahub.upgrade.propagate;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaMetadata;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@RequiredArgsConstructor
class EntityFetcher {
  private final EntityService _entityService;

  private static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(Constants.SCHEMA_METADATA_ASPECT_NAME, Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);

  Map<Urn, EntityDetails> fetchSchema(Set<Urn> urns) {
    try {
      return _entityService.getEntitiesV2(Constants.DATASET_ENTITY_NAME, urns, ASPECTS_TO_FETCH)
          .entrySet()
          .stream()
          .collect(Collectors.toMap(Map.Entry::getKey, entry -> transformResponse(entry.getValue())));
    } catch (URISyntaxException e) {
      log.error("Error while fetching schema for a batch of urns", e);
      return Collections.emptyMap();
    }
  }

  private EntityDetails transformResponse(EntityResponse entityResponse) {
    if (!entityResponse.getAspects().containsKey(Constants.SCHEMA_METADATA_ASPECT_NAME)) {
      return new EntityDetails(entityResponse.getUrn(), null, null);
    }
    SchemaMetadata schemaMetadata =
        new SchemaMetadata(entityResponse.getAspects().get(Constants.SCHEMA_METADATA_ASPECT_NAME).getValue().data());

    if (!entityResponse.getAspects().containsKey(Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME)) {
      return new EntityDetails(entityResponse.getUrn(), schemaMetadata, null);
    }
    EditableSchemaMetadata editableSchemaMetadata = new EditableSchemaMetadata(
        entityResponse.getAspects().get(Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME).getValue().data());
    return new EntityDetails(entityResponse.getUrn(), schemaMetadata, editableSchemaMetadata);
  }
}
